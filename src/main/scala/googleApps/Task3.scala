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

    // Replace null values with 0
    val df_no_null = DataCleaner(df_rating).removeNullsFromColumn("Rating")

    // Size as double and transformations
    val df_size = df_no_null
      .withColumn("Size",
        when(col("Size").endsWith("M"), regexp_replace(col("Size"), "M", "").cast("double"))
          .otherwise(lit(null).cast("double"))
      )

    // Convert price to euro and cast to Double
    val df_price = df_size.withColumn("Price",
      when(col("Price").contains("$"), regexp_replace(col("Size"), "\\$", "").cast("double") * 0.9)
        .otherwise(col("Price").cast("double"))
    )

    // Order reviews desc
    val df_order = df_price.orderBy(col("Reviews").desc)

    // Aggregations
    val df_aggregations = Aggregations(df_order).aggregateAndGroupByApp()

    // Convert Last_Updated to Date
    val df_date = df_aggregations.withColumn("Last_Updated", to_date(col("Last_Updated"), "MMMM dd, yyyy").cast("Date"))

    // Convert ";" to ","
    val df_3 = df_date.withColumn("Genres", expr("transform(genres, x -> regexp_replace(x, '\\s*;\\s*', ','))"))

    //Return result
    df_3
  }


}
