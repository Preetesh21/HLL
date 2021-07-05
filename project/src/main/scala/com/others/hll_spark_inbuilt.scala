package com.others

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.approx_count_distinct

object hll_spark_inbuilt {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("Spark")
      .getOrCreate()
    // Reading the csv file => Change the path if needed.
    //    val df = spark.read.format("csv").option("header", "true")
    //      .load("wasbs://aspahdiclustehdistorage.blob.core.windows.net/aspahdicluster-2020-02-25t09-19-23-989z/work_leave.csv")
    val df = spark.read.format("csv")
      .option("header", "true")
      .load("wasbs://aspahdicluster-2020-02-25t09-19-23-989z@aspahdiclustehdistorage.blob.core.windows.net/work_leave.csv")
    // Using the approx_count method based on the HLL algorithm.
    df.agg(approx_count_distinct("ID").as("approx_user_id_count")).show()
    spark.stop()
  }
}
