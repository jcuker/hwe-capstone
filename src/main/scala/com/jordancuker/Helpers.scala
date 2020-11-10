package com.jordancuker

import com.jordancuker.schema.{EnrichedData, HBaseUserInfo, KafkaReviewCaseClass}
import Capstone.{env, logger}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Get}
import org.apache.hadoop.hbase.util.Bytes

object Helpers {
  implicit def stringToBytes(s: String): Array[Byte] = Bytes.toBytes(s)
  implicit def bytesToString(b: Array[Byte]): String = Bytes.toString(b)

  def processReviews(session: SparkSession, reviews: Dataset[String]): Dataset[EnrichedData] = {
    import session.implicits._

    reviews
      .map(reviewTabSeparated => reviewTabSeparated.split('\t'))
      .map(split => convertSplitArrayIntoKafkaReviewClass(split))
      .mapPartitions(review => enrichKafkaReviewClassWithHBaseUserInfo(review))
  }

  def convertSplitArrayIntoKafkaReviewClass(split: Array[String]): KafkaReviewCaseClass = {
    val marketplace: String = split(0)
    val customer_id: String = split(1)
    val review_id: String = split(2)
    val product_id: String = split(3)
    val product_parent: String = split(4)
    val product_title: String = split(5)
    val product_category: String = split(6)
    val star_rating: Int = split(7).toInt
    val helpful_votes: Int = split(8).toInt
    val total_votes: Int = split(9).toInt
    val vine: String = split(10)
    val verified_purchase: String = split(11)
    val review_headline: String = split(12)
    val review_body: String = split(13)
    val review_date: String = split(14)

    KafkaReviewCaseClass(marketplace,
      customer_id,
      review_id,
      product_id,
      product_parent,
      product_title,
      product_category,
      star_rating,
      helpful_votes,
      total_votes,
      vine,
      verified_purchase,
      review_headline,
      review_body,
      review_date)
  }

  def enrichKafkaReviewClassWithHBaseUserInfo(reviewPartition: Iterator[KafkaReviewCaseClass]): Iterator[EnrichedData] = {
    val conf: Configuration = HBaseConfiguration.create()
    val hbaseZookeeperQuorum = env.get("HBASE_ZOOKEEPER_QUORUM")
    conf.set("hbase.zookeeper.quorum", hbaseZookeeperQuorum)

    var connection: Connection = null
    var table: org.apache.hadoop.hbase.client.Table = null
    var returnValue: Iterator[EnrichedData] = null

    try {
      connection = ConnectionFactory.createConnection(conf)
      val hbaseTableName = env.get("HBASE_TABLE_NAME")
      table = connection.getTable(TableName.valueOf(hbaseTableName))

      returnValue = reviewPartition.map((review) => {
        val get = new Get(review.customer_id)
        val result = table.get(get)

        val customerName = Bytes.toString(result.getValue("f1", "name"))
        val customerBirthdate = Bytes.toString(result.getValue("f1", "birthdate"))
        val customerMail = Bytes.toString(result.getValue("f1", "mail"))
        val customerSex = Bytes.toString(result.getValue("f1", "sex"))
        val customerUsername = Bytes.toString(result.getValue("f1", "username"))

        val hBaseUserInfoCaseClass = HBaseUserInfo(customerName, customerBirthdate, customerMail, customerSex, customerUsername)
        val enrichedData = EnrichedData(review, hBaseUserInfoCaseClass)

        println("Processed a new record: " + enrichedData)
        enrichedData
      })
    } catch {
      case e: Exception => logger.error("Error in connecting to HBase", e)
    }

    returnValue
  }

}
