package org.googleApp.utils

import org.apache.spark.sql.SparkSession

trait SharedSparkInstance {

  implicit  val spark = SparkSession
    .builder()
    .appName("GoogleApps")
    .master("local[*]")
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
    .getOrCreate()
}
