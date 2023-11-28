package googleApps

import googleApps.utils.SharedSparkInstance
import googleApps.utils.sink.ParquetSink
import googleApps.utils.transformations.Aggregations
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.dsl.expressions.StringToAttributeConversionHelper
import org.apache.spark.sql.functions._



class Task5 extends App with SharedSparkInstance {

  def process(df_3_popularity: DataFrame) = {

    //val explodedDF = df_3.withColumn("Genres", explode(col("Genres")).as("Genre"))

    //
    val explodedDF = df_3_popularity
      .select(
        col("App"),
        explode(col("Genres")).as("Genre"),
        col("Rating"),
        col("Average_Sentiment_Polarity"))

    // Group by Genre and count the number of Apps for each genre

    val df_04 = Aggregations(explodedDF).groupByGenre()


    ParquetSink.writeDataCompressed("src/main/data/sink/metrics/googleplaystore_metrics", df_04)
    df_04
  }
}
