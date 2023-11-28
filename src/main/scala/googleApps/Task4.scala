package googleApps

import googleApps.utils.SharedSparkInstance
import googleApps.utils.sink.ParquetSink
import org.apache.spark.sql.DataFrame

class Task4 extends App with SharedSparkInstance {

  def process(df_1: DataFrame, df_3: DataFrame) = {

    val df_joined = df_1.join(df_3,"App")

    ParquetSink.writeData("src/main/data/sink/join", df_joined)

    df_joined
  }
}
