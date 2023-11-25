package googleApps.utils.schemas

import org.apache.spark.sql.types.{ArrayType, DateType, DoubleType, LongType, StringType, StructField, StructType}

object GooglePlayStoreSchema {
  val schema = StructType(Array(
    StructField("App", StringType),
    StructField("Category", StringType),
    StructField("Rating", DoubleType),
    StructField("Reviews", LongType),
    StructField("Size", DoubleType),
    StructField("Installs", StringType),
    StructField("Type", StringType),
    StructField("Price", DoubleType),
    StructField("Content_Rating", StringType),
    StructField("Genres", ArrayType(StringType)),
    StructField("Last_Updated", DateType),
    StructField("Current_version", StringType),
    StructField("Minimum_Android_Version", StringType),
  ))
}
