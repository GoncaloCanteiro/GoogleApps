package googleApps.utils.schemas

import org.apache.spark.sql.types.{DoubleType, StructField, StructType}

class GoogleReviewAvgSentimentPolaritySchema {
  val schema = StructType(Array(
    StructField("Average_Sentiment_Polarity", DoubleType),
  ))
}
