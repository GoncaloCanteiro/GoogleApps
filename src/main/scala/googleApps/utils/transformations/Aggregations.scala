package googleApps.utils.transformations

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

case class Aggregations(df: DataFrame) {

  def aggregateByApp(): DataFrame = {
    df.groupBy(col("App")).avg("Sentiment_Polarity")
  }
}
