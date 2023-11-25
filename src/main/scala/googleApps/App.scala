package googleApps

import googleApps.utils.SharedSparkInstance
import googleApps.utils.ingestion.CsvReader
import org.apache.spark.sql.functions._
import googleApps.utils.schemas.{GooglePlayStoreSchema, GooglePlaystoreUserReviewsSchema, GoogleReviewAvgSentimentPolaritySchema}
import googleApps.utils.sink.CsvSink
import googleApps.utils.transformations.{Aggregations, DataCleaner}

object App extends App with SharedSparkInstance {

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

  // Load data from CSV googleplaystore.csv
  val df_googleplaystore = CsvReader.loadDataAutoSchema("src/main/data/googleplaystore.csv")

  // Cast "Rating" column to Double type
  val dfWithRating = df_googleplaystore.withColumn("Rating", col("Rating").cast("Double"))

  // Replace null values with 0
  val df_no_nan = DataCleaner(dfWithRating).removeNullsFromColumn("Rating")

  // Filter Apps with Rating greater or equal to 4.0 and lower then 5
  val df_filtered_apps = df_no_nan.filter(col("Rating") >= 4.0 && col("Rating") <= 5.0)

  // Sort the DataFrame in descending order based on the "Rating" column
  val df_sorted_apps = df_filtered_apps.sort(col("Rating").desc)

  // Write Dataframe to CSV
  CsvSink.writeData("src/main/data/bestAppsSink", df_sorted_apps)

  df_sorted_apps.show()

  spark.stop()
}
