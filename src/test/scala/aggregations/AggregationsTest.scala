import org.googleApp.utils.transformations.Aggregations
import org.scalatest.FunSuite
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

class AggregationsTest extends FunSuite {

  private val spark: SparkSession = SparkSession.builder().appName("AggregationsTest").master("local[2]").getOrCreate()

  test("groupByApp should group by App and calculate average Sentiment_Polarity") {
    import spark.implicits._

    // Sample input data
    val inputData: DataFrame = Seq(
      ("App1", 4.0),
      ("App1", 3.0),
      ("App2", 0.8),
      ("App2", 0.2),
      ("App3", 0.0)
    ).toDF("App", "Sentiment_Polarity")

    // Expected output data
    val expectedOutput: DataFrame = Seq(
      ("App1", 3.5),
      ("App2", 0.5),
      ("App3", 0.0)
    ).toDF("App", "avg(Sentiment_Polarity)")

    // Apply the function to the input data
    val result: DataFrame = Aggregations(inputData).groupByApp().orderBy(col("App"))

    // Assert that the result matches the expected output
    assert(result.collectAsList() === expectedOutput.collectAsList())
  }

  test("groupByGenre should group by Genre and perform aggregations") {
    import spark.implicits._

    // Sample input data
    val inputData: DataFrame = Seq(
      ("Game", "App1", 4.5, 0.5),
      ("Game", "App2", 3.8, -0.5),
      ("Social", "App3", 4.2, 0.8),
      ("Social", "App4", 3.5, 0.2),
      ("Education", "App5", 4.0, 0.0)
    ).toDF("Genre", "App", "Rating", "Average_Sentiment_Polarity")

    // Expected output data
    val expectedOutput: DataFrame = Seq(
      ("Game", 2, 4.15, 0.0),
      ("Social", 2, 3.85, 0.5),
      ("Education", 1, 4.0, 0.0)
    ).toDF("Genre", "Count", "Average_Rating", "Average_Sentiment_Polarity")


    // Apply the function to the input data
    val result: DataFrame = Aggregations(inputData).groupByGenre()

    // Sort both expected and actual results
    val sortedExpected: DataFrame = expectedOutput.sort("Genre")
    val sortedResult: DataFrame = result.sort("Genre")

    // Assert that the result matches the expected output
    assert(sortedResult.collectAsList() === sortedExpected.collectAsList())
  }
}
