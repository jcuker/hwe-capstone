package com.jordancuker

import com.jordancuker.schema.EnrichedData
import io.github.cdimascio.dotenv.Dotenv
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.logging.log4j.LogManager

object Capstone {
  implicit def stringToBytes(s: String): Array[Byte] = Bytes.toBytes(s)
  implicit def bytesToString(b: Array[Byte]): String = Bytes.toString(b)

  lazy val logger: org.apache.logging.log4j.Logger = LogManager.getLogger(this.getClass)

  val appName = "HWE-Capstone-Project"
  val env: Dotenv = Dotenv.load()

  def main(args: Array[String]): Unit = {
    try {
      System.setProperty("HADOOP_USER_NAME", env.get("HADOOP_USER_NAME"))

      val spark = SparkSession.builder()
        .appName(appName)
        .config("spark.sql.shuffle.partitions", "3")
        .config("spark.hadoop.dfs.client.use.datanode.hostname", "true")
        .config("spark.hadoop.fs.defaultFS", env.get("HADOOP_DEFAULT_FS"))
        .master("local[*]")
        .getOrCreate()

      import spark.implicits._

      val bootstrapServers = env.get("KAFKA_BOOTSTRAP_SERVERS")
      val sentences = spark
        .readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrapServers)
        .option("subscribe", "reviews")
        .load()
        .selectExpr("CAST(value AS STRING)").as[String]

      val enrichedData: Dataset[EnrichedData] = Helpers.processReviews(spark, sentences)

      val hdfsCheckpointLocation = env.get("HDFS_CHECKPOINT_LOCATION")
      val writeEnrichedData = enrichedData.writeStream
        .format("parquet")
        .option("path", env.get("HDFS_PATH"))
        .option("checkpointLocation", hdfsCheckpointLocation)
        .outputMode(OutputMode.Append())
        .trigger(Trigger.ProcessingTime("30 seconds"))
        .start()

      writeEnrichedData.awaitTermination()
    } catch {
      case e: Exception => logger.error(s"$appName error in main", e)
    }
  }
}
