package googleApps

import googleApps.utils.SharedSparkInstance
import googleApps.utils.ingestion.CsvReader
import org.apache.spark.sql.functions._
import googleApps.utils.schemas.{GooglePlayStoreSchema, GooglePlaystoreUserReviewsSchema, GoogleReviewAvgSentimentPolaritySchema}
import googleApps.utils.transformations.{Aggregations, DataCleaner}

object App extends App with SharedSparkInstance {

  // Load data from CSV with schema
  val df_google_reviews = CsvReader.loadData("src/main/data/googleplaystore_user_reviews.csv",GooglePlaystoreUserReviewsSchema.schema)
  //val df_cast = df_google_reviews.withColumn("Sentiment_Polarity", col("Sentiment_Polarity").cast(DoubleType))

  // Replace null values with 0
  val df_no_null = DataCleaner(df_google_reviews).removeNan()

  // Calculate Avg of "Sentiment_Polarity" grouped by "App"
  val df_1 = Aggregations(df_no_null).aggregateByApp()

  //show result
  df_1.show(40, false)

  spark.stop()
}
