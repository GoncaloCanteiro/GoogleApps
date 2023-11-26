package googleApps

import googleApps.utils.SharedSparkInstance
import googleApps.utils.ingestion.CsvReader
import googleApps.utils.sink.CsvSink
import googleApps.utils.transformations.DataCleaner
import org.apache.spark.sql.functions.col

object Task2 extends App with SharedSparkInstance {
  // Load data from CSV googleplaystore.csv
  val df_googleplaystore = CsvReader.loadDataAutoSchema("src/main/data/googleplaystore.csv")

  // Cast "Rating" column to Double type
  val dfWithRating = df_googleplaystore.withColumn("Rating", col("Rating").cast("Double"))

  // Remove Nulls
  val df_no_nul = DataCleaner(dfWithRating).removeNullsFromColumn("Rating")

  // Filter Apps with Rating greater or equal to 4.0 and lower then 5
  val df_filtered_apps = df_no_nul.filter(col("Rating") >= 4.0 && col("Rating") <= 5.0)

  // Sort the DataFrame in descending order based on the "Rating" column
  val df_sorted_apps = df_filtered_apps.sort(col("Rating").desc)

  // Write Dataframe to CSV
  CsvSink.writeData("src/main/data/bestAppsSink", df_sorted_apps)

  df_sorted_apps.show()
}
