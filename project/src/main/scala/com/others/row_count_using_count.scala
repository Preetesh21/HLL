package com.others

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.countDistinct

object row_count_using_count {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .master("local[8]")
      .appName("SparkKMeans")
      .getOrCreate()
    // Reading the csv file => Change the path if needed.
    val df = spark.read.format("csv").option("header", "true").load("./src/main/resources/work_leave.csv")
    // Using the count method based on the HLL algorithm.
    println(df.agg(countDistinct("ID")).show())
    spark.stop()
  }
}
