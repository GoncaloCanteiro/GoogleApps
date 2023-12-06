package org.googleApp.utils.schemas

import org.apache.spark.sql.types.{ DoubleType, StringType, StructField, StructType}

object GooglePlaystoreUserReviewsSchema {
  val schema = StructType(Array(
    StructField("App", StringType),
    StructField("Translated_Review", StringType),
    StructField("Sentiment", StringType),
    StructField("Sentiment_Polarity", DoubleType),
    StructField("Sentiment_Subjectivity", DoubleType),

  ))

}
