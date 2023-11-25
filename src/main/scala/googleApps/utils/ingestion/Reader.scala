package googleApps.utils.ingestion

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType

trait Reader {
  def loadData(path: String, schema: StructType): DataFrame
  def loadDataAutoSchema(path: String): DataFrame

}
