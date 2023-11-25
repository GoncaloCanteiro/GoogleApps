package googleApps.utils.transformations

import org.apache.spark.sql.DataFrame

case class DataCleaner(df: DataFrame) {

  def removeNan(): DataFrame = {
    df.na.fill(0)
  }
}
