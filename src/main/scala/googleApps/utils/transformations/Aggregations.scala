package googleApps.utils.transformations

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

case class Aggregations(df: DataFrame) {

  def groupByApp(): DataFrame = {
    df.groupBy(col("App")).avg("Sentiment_Polarity")
  }

  def aggregateAndGroupByApp(): DataFrame = {
    df.groupBy("App").agg(
      collect_set("Category").as("Categories"),
      first("Rating").as("Rating"),
      first("Reviews").as("Reviews"),
      first("Size").as("Size"),
      first("Installs").as("Installs"),
      first("Type").as("Type"),
      first("Price").as("Price"),
      first("Content Rating").as("Content_Rating"),
      collect_list("Genres").as("Genres"),
      first("Last Updated").as("Last_Updated"),
      first("Current Ver").as("Current_Version"),
      first("Android Ver").as("Minimum_Android_Version"),
    )
  }

  def groupByGenre(): DataFrame = {
    df.groupBy("Genre")
      .agg(count("App").alias("Count"),
        avg("Rating").as("Average_Rating"),
        avg("Average_Sentiment_Polarity").as("Average_Sentiment_Polarity"))
  }
}
