package googleApps

import googleApps.utils.SharedSparkInstance
import googleApps.utils.ingestion.CsvReader
import org.apache.spark.sql.functions._
import googleApps.utils.transformations.{DataCleaner}


object Task3 extends App with SharedSparkInstance {

  val df_googleplaystore = CsvReader.loadDataAutoSchema("src/main/data/googleplaystore.csv")

  // Cast "Rating" column to Double type
  val dfWithRating = df_googleplaystore.withColumn("Rating", col("Rating").cast("Double"))

  // Replace null values with 0
  val df_no_null = DataCleaner(dfWithRating).removeNullsFromColumn("Rating")

  // Size as double and transformations
  val castSizeDF= df_no_null
    .withColumn("Size",
      when(col("Size").endsWith("M"), regexp_replace(col("Size"), "M", "").cast("double"))
        .otherwise(lit(null).cast("double"))

    )


  val orderDF = castSizeDF.orderBy(col("Reviews").desc)

  // add comment
  val resultDF = orderDF.groupBy("App").agg(
    collect_set("Category").as("Categories"),
    first("Rating").as("Rating"),
    first("Reviews").as("Reviews"),
    first("Size").as("Size"),
    first("Installs").as("Installs"),
    first("Type").as("Type"),
    first("Price").as("Price"),
    first("Content Rating").as("Content_Rating"),
    collect_set("Genres").as("Genres"),
    first("Last Updated").as("Last_Updated"),
    first("Current Ver").as("Current_Version"),
    first("Android Ver").as("Minimum_Android_Version"),
  )
    .orderBy(col("Reviews").desc)

  resultDF.show()
  resultDF.printSchema()

  spark.stop()
}
