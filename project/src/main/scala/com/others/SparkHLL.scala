package com.others

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.approx_count_distinct

/**
 * This object file is showing the way of using the inbuilt HLL functionality provided by Spark
 * to calculate the row count of a column in a dataset.
 */

object SparkHLL {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .appName("Spark")
      .getOrCreate()

    val dataframe = spark.read.format("csv")
      .option("header", "true")
      .load("wasbs://aspahdicluster-2020-02-25t09-19-23-989z@aspahdiclustehdistorage.blob.core.windows.net/work_leave.csv")

    // Using the approx_count method based on the HLL algorithm.
    dataframe.agg(approx_count_distinct("ID").as("approx_user_id_count")).show()
    spark.stop()
  }
}
