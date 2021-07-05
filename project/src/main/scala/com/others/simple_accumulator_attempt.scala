package com.others

import org.apache.spark.sql.SparkSession

object simple_accumulator_attempt {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("SparkKMeans")
      .getOrCreate()
    val sc = spark.sparkContext
    // Reading the csv file => Change the path if needed.
    val df = spark.read.format("csv").option("header", "true").load(args(0).toString())
    // The accumulator
    var accum = sc.accumulator(0)
    val df2 = df.groupBy("ID").count().foreach(Row => accum += 1)
    println(accum)
    spark.stop()
  }
}
