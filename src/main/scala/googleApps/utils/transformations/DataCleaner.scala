package googleApps.utils.transformations

import org.apache.spark.sql.DataFrame

case class DataCleaner(df: DataFrame) {

  def replaceNanWithZero(): DataFrame = {
    df.na.fill(0)
  }

  def removeNullsFromColumn(columnName: String): DataFrame = {
    df.na.drop(Seq(columnName))
  }
}