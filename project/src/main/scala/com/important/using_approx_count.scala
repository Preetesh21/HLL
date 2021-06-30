package com.important

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object using_approx_count {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]")
      .setAppName("RDD_Movie_Users_Analyzer")

    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext
    var df = spark.read.format("csv").option("header", "true").load("./src/main/resources/work_leave.csv").toDF()

    val expressions = df.columns.slice(0,1).map((_ -> "approx_count_distinct")).toMap
    df.agg(expressions).show()
    spark.stop()
  }
}
