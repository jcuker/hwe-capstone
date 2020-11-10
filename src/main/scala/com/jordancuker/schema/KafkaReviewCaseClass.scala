package com.jordancuker.schema

case class KafkaReviewCaseClass(
                                 marketplace: String,
                                 customer_id: String,
                                 review_id: String,
                                 product_id: String,
                                 product_parent: String,
                                 product_title: String,
                                 product_category: String,
                                 star_rating: Int,
                                 helpful_votes: Int,
                                 total_votes: Int,
                                 vine: String,
                                 verified_purchase: String,
                                 review_headline: String,
                                 review_body: String,
                                 review_date: String)
