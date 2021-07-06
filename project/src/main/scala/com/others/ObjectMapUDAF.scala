package com.others

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

/**
 * This object file is showing the way of using the UDAF to calculate the row count
 * of a column in a dataset.
 */

object ObjectMapUDAF {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Demo3")
      .getOrCreate()

    // Reading the csv file => Change the path if needed.
    val dataframe = spark.read.format("csv")
      .option("header", "true")
      .load("wasbs://aspahdicluster-2020-02-25t09-19-23-989z@aspahdiclustehdistorage.blob.core.windows.net/work_leave.csv")

    val udafObject = new MapUDAF
    dataframe.agg(udafObject(col("ID")).as("Count")).show()
    spark.stop()
  }
}
