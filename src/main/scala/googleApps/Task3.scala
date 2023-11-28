package googleApps

import googleApps.utils.SharedSparkInstance
import googleApps.utils.ingestion.CsvReader
import org.apache.spark.sql.functions._
import googleApps.utils.transformations.{Aggregations, DataCleaner}
import org.apache.spark.sql.DataFrame

class Task3 extends App with SharedSparkInstance {

  def process(): DataFrame = {
    val df_googleplaystore = CsvReader.loadDataAutoSchema("src/main/data/googleplaystore.csv")

    // cast rating to double
    val df_rating = df_googleplaystore.withColumn("Rating", col("Rating").cast("Double"))

    // cast review to long
    val df_reviews = df_rating.withColumn("Reviews", col("Reviews").cast("Long"))

    // Size as double and transformations
    val df_size = DataCleaner(df_reviews).cleanSize()

    // Convert price to euro and cast to Double
    val df_price = DataCleaner(df_size).cleanPrice()

    // Order reviews desc
    val df_order = df_price.orderBy(col("Reviews").desc)

    // split genres by ";" and explode genres
    val df_change_delimiter = DataCleaner(df_order).changeDelimiter()

    // Aggregations group by app
    val df_aggregations = Aggregations(df_change_delimiter).aggregateAndGroupByApp()

    // Convert Last_Updated to Date
    val df_date = DataCleaner(df_aggregations).date()

    // Remove duplicate Genres in the array
    val df_3 = df_date.withColumn("Genres", array_distinct(col("Genres")))

    //Return result
    df_3
  }


}
