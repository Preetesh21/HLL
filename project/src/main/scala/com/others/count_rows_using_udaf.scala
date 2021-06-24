package com.others

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object count_rows_using_udaf {
  def main(args: Array[String]): Unit = {
    println("Hello world")
    val spark = SparkSession.builder().master("local[8]").appName("Demo3").getOrCreate()
    var sc = spark.sparkContext
    // Reading the csv file => Change the path if needed.

    val inputDF = spark.read.format("csv").load("./src/main/resources/sample.csv").toDF("col1", "col2", "col3")
    val gm = new UDAF_Calculate
    //inputDF.show()
    inputDF.groupBy("col2").agg(gm(col("col2")).as("Count")).show()
    spark.stop()
  }
}
