import googleApps.{Task1, Task2, Task3, Task4, Task5}
import googleApps.utils.SharedSparkInstance
import org.apache.spark.sql.functions._

object MainExecution extends App with SharedSparkInstance{

  // Run Task1
  println("Running Task1 ...")
  val df_1 = new Task1().process()
  df_1.show()

  // Run Task2
  println("Running Task2 ...")
  val df_2 = new Task2().process()
  df_2.show()

  // Run Task3
  println("Running Task3 ...")
  val df_3 = new Task3().process()

  // Run Task4
  println("Running Task4 ...")
  val df_3_popularity = new Task4().process(df_1, df_3)

  // Run Task5
  println("Running Task5 ...")
  val df_4 = new Task5().process(df_3_popularity)
  df_4.show(20, false)

  spark.stop()

}
