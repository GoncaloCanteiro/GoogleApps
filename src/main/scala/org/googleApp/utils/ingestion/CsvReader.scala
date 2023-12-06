package org.googleApp.utils.ingestion

import org.googleApp.utils.SharedSparkInstance
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType

object CsvReader extends Reader with SharedSparkInstance{
  override def loadData(path: String, schema: StructType): DataFrame = {
    spark.read
      .option("header", value = true)
      .schema(schema)
      .csv(path)
  }

  override def loadDataAutoSchema(path: String): DataFrame = {
    spark.read
      .option("header", value = true)
      .option("inferSchema","true")
      .csv(path)
  }
}
