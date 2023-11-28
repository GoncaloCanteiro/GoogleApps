package googleApps.utils.sink

import googleApps.utils.SharedSparkInstance
import org.apache.spark.sql.DataFrame

object ParquetSink extends Parquet with SharedSparkInstance {
  override def writeData(path: String, df: DataFrame): Unit = {
    df.write
      .mode("overwrite")
      .option("header", value = true)
      .parquet(path)
  }

  override def writeDataCompressed(path: String, df: DataFrame): Unit = {
    df.write
      .mode("overwrite")
      .option("header", value = true)
      .option("compression", "gzip")
      .parquet(path)
  }
}


