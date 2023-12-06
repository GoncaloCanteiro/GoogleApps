package org.googleApp

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.googleApp.utils.SharedSparkInstance
import org.googleApp.utils.ingestion.CsvReader
import org.googleApp.utils.sink.CsvSink
import org.googleApp.utils.transformations.DataCleaner

class Task2 extends App with SharedSparkInstance {
  def process(): DataFrame = {
    // Load data from CSV googleplaystore.csv
    val df_googleplaystore = CsvReader.loadDataAutoSchema("data/googleplaystore.csv")

    // Cast "Rating" column to Double type
    val dfWithRating = df_googleplaystore.withColumn("Rating", col("Rating").cast("Double"))

    // Remove Nulls from column Rating
    val df_no_nul = DataCleaner(dfWithRating).removeNullsFromColumn("Rating")

    // Filter Apps with Rating greater or equal to 4.0 and lower then 5
    val df_filtered_apps = df_no_nul.filter(col("Rating") >= 4.0 && col("Rating") <= 5.0)

    // Sort the DataFrame in descending order based on the "Rating" column
    val df_sorted_apps = df_filtered_apps.sort(col("Rating").desc)

    // Write Dataframe as CSV
    CsvSink.writeData("src/main/data/sink/bestApps", df_sorted_apps)

    df_sorted_apps
  }

}
