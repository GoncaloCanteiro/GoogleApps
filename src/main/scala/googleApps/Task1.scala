package googleApps

import googleApps.utils.SharedSparkInstance
import googleApps.utils.ingestion.CsvReader
import org.apache.spark.sql.functions._
import googleApps.utils.schemas.{ GooglePlaystoreUserReviewsSchema}
import googleApps.utils.transformations.{Aggregations, DataCleaner}


object Task1 extends App with SharedSparkInstance {

  // Load data from CSV with schema
  val df_google_reviews = CsvReader.loadData("src/main/data/googleplaystore_user_reviews.csv",GooglePlaystoreUserReviewsSchema.schema)
  //val df_cast = df_google_reviews.withColumn("Sentiment_Polarity", col("Sentiment_Polarity").cast(DoubleType))

  // Calculate Avg of "Sentiment_Polarity" grouped by "App"
  val df_google_reviews_aggregations = Aggregations(df_google_reviews).aggregateByApp()

  // Replace null values with 0
  val df_1 = DataCleaner(df_google_reviews_aggregations).replaceNanWithZero()

  //show df_1 result

  df_1
    .select(col("App"),col("avg(Sentiment_Polarity)")
    .alias("Average_Sentiment_Polarity"))
    .show(40, false)

  spark.stop()
}
