package org.googleApp.utils.sink

import org.googleApp.utils.SharedSparkInstance
import org.apache.spark.sql.DataFrame

object CsvSink extends Csv with SharedSparkInstance{
  override def writeData(path: String, df: DataFrame): Unit = {
    df.write
      .mode("overwrite")
      .option("delimiter", "§")
      .option("header", value = true)
      .csv(path)
  }
}
