package googleApps.utils.transformations

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

case class DataCleaner(df: DataFrame) {

  def replaceNanWithZero(): DataFrame = {
    df.na.fill(0)
  }

  def removeNullsFromColumn(columnName: String): DataFrame = {
    df.na.drop(Seq(columnName))
  }

  // Size as double and transformations
  def cleanSize(): DataFrame = {
    df.withColumn("Size",
      when(col("Size").endsWith("M"), regexp_replace(col("Size"), "M", "").cast("double"))
        .otherwise(lit(null).cast("double"))
    )
  }

  // Convert price to euro and cast to Double
  def cleanPrice(): DataFrame = {
    df.withColumn("Price",
      when(col("Price").contains("$"), regexp_replace(col("Size"), "\\$", "").cast("double") * 0.9)
        .otherwise(col("Price").cast("double"))
    )
  }

  // Change delimiter and explode genres
  def changeDelimiter(): DataFrame = {
    df.withColumn("Genres", split(col("Genres"), ";"))
      .withColumn("Genres", explode(col("Genres")))
  }

  def date(): DataFrame = {
    df.withColumn("Last_Updated", to_date(col("Last_Updated"), "MMMM dd, yyyy")
      .cast("Date"))
  }

}
