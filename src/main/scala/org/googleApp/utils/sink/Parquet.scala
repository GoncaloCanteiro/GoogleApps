package org.googleApp.utils.sink

import org.apache.spark.sql.DataFrame

trait Parquet {
  def writeData(path: String, df: DataFrame): Unit
  def writeDataCompressed(path: String, df: DataFrame): Unit
}
