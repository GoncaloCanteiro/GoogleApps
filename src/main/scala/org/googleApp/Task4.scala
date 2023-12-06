package org.googleApp

import org.apache.spark.sql.DataFrame
import org.googleApp.utils.SharedSparkInstance
import org.googleApp.utils.sink.ParquetSink

class Task4 extends App with SharedSparkInstance {

  def process(df_1: DataFrame, df_3: DataFrame) = {

    val df_joined = df_1.join(df_3,"App")

    ParquetSink.writeData("data/sink/join", df_joined)

    df_joined
  }
}
