package com.others

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object using_approx_count {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("Spark")

    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext
    val df = spark.read.format("csv")
      .option("header", "true")
      .load("wasbs://aspahdicluster-2020-02-25t09-19-23-989z@aspahdiclustehdistorage.blob.core.windows.net/work_leave.csv")
    val expressions = df.columns.slice(0, 1).map((_ -> "approx_count_distinct")).toMap
    println(df.agg(expressions).show())
    spark.stop()
  }
}
