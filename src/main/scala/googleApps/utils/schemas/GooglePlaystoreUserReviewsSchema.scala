package googleApps.utils.schemas

import org.apache.spark.sql.types.{ArrayType, DateType, DoubleType, LongType, StringType, StructField, StructType}

object GooglePlaystoreUserReviewsSchema {
  val schema = StructType(Array(
    StructField("App", StringType),
    StructField("Translated_Review", StringType),
    StructField("Sentiment", StringType),
    StructField("Sentiment_Polarity", DoubleType),
    StructField("Sentiment_Subjectivity", DoubleType),

  ))

}
