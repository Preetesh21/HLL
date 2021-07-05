package com.others

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.countDistinct

object row_count_using_count {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("SparkKMeans")
      .getOrCreate()
    // Reading the csv file => Change the path if needed.
    //val df = spark.read.format("csv").option("header", "true").load("https://aspahdiclustehdistorage.blob.core.windows.net/aspahdicluster-2020-02-25t09-19-23-989z/work_leave.csv")
    val df = spark.read.format("csv")
      .option("header", "true")
      .load("wasbs://aspahdicluster-2020-02-25t09-19-23-989z@aspahdiclustehdistorage.blob.core.windows.net/work_leave.csv")
    // Using the count method based on the HLL algorithm.
    println(df.agg(countDistinct("ID")).show())
    spark.stop()
  }
}
