package com.others

import com.cardinality.HyperLogLogPlusAggregator
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object using_hll_udaf {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("SparkKMeans")
      .getOrCreate()
    // Reading the csv file => Change the path if needed.
    // var df = spark.read.format("csv").option("header", "true").load("https://aspahdiclustehdistorage.blob.core.windows.net/aspahdicluster-2020-02-25t09-19-23-989z/work_leave.csv")
    val df = spark.read.format("csv")
      .option("header", "true")
      .load("wasbs://aspahdicluster-2020-02-25t09-19-23-989z@aspahdiclustehdistorage.blob.core.windows.net/work_leave.csv")
    // The UDAF
    val gm = new HyperLogLogPlusAggregator
    println(df.agg(gm(col("ID"))).as("Count").show())
    //scala.io.StdIn.readLine()
    spark.stop()
    // df.agg(approx_count_distinct("Count").as("approx_user_id_count")).show()
  }
}
