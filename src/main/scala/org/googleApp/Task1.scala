package org.googleApp

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.googleApp.utils.ingestion.CsvReader
import org.googleApp.utils.schemas.GooglePlaystoreUserReviewsSchema
import org.googleApp.utils.transformations.{Aggregations, DataCleaner}


class Task1{

  def process(): DataFrame = {
    // Load data from CSV with schema
    val df_google_reviews = CsvReader.loadData("data/googleplaystore_user_reviews.csv", GooglePlaystoreUserReviewsSchema.schema)

    // Replace null values with 0
    val df_clean = DataCleaner(df_google_reviews).replaceNanWithZero()

    // Calculate Avg of "Sentiment_Polarity" grouped by "App"
    val df_1 = Aggregations(df_clean).groupByApp()

    //Return df_1 result
    df_1
      .select(col("App"), col("avg(Sentiment_Polarity)")
        .alias("Average_Sentiment_Polarity"))
  }


}
