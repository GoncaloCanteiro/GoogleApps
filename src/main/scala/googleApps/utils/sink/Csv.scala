package googleApps.utils.sink

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType

trait Csv {
  def writeData(path: String, df: DataFrame): Unit
}
