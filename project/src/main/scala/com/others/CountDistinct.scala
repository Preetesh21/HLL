package com.others

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.countDistinct


/**
 * This object file is showing the way of using the inbuilt functionality 'count' provided by Spark
 * to calculate the row count of a column in a dataset.
 */

object CountDistinct {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("SparkKMeans")
      .getOrCreate()

    // Reading the csv file => Change the path if needed.
    val df = spark.read.format("csv")
      .option("header", "true")
      .load("wasbs://aspahdicluster-2020-02-25t09-19-23-989z@aspahdiclustehdistorage.blob.core.windows.net/work_leave.csv")

    // Using the count method based on the HLL algorithm.
    df.agg(countDistinct("ID")).show()

    spark.stop()
  }
}
