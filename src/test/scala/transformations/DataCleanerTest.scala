package transformations
import org.googleApp.utils.transformations.DataCleaner
import org.scalatest.FunSuite
import org.apache.spark.sql.{DataFrame, SparkSession}

class DataCleanerTest extends FunSuite {

  private val spark: SparkSession = SparkSession.builder().appName("DataCleanerTest").master("local[2]").getOrCreate()

  test("replaceNanWithZero should replace NaN values with 0") {
    import spark.implicits._

    // Sample test data
    val inputData: DataFrame = Seq(
      (1, "A", 3.0),
      (2, "B", Double.NaN),
      (3, "C", 5.0)
    ).toDF("ID", "Category", "Value")
    // Apply the function to the input data
    val result: DataFrame = DataCleaner(inputData).replaceNanWithZero()

    // Expected output data
    val expectedOutput: DataFrame = Seq(
      (1, "A", 3.0),
      (2, "B", 0.0),
      (3, "C", 5.0)
    ).toDF("ID", "Category", "Value")

    // Assert that the result matches the expected output
    assert(result.collectAsList() === expectedOutput.collectAsList())
  }




}
