package com.important

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object using_hll_accumulator {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]")
      .setAppName("RDD_Movie_Users_Analyzer")

    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext

    var df = spark.read.format("csv").option("header", "true").load("./src/main/resources/work_leave.csv").toDF()
    var vals: List[String] = df.select("ID").collect().map(_ (0)).toList.map(_.toString)
    val gm = new HLLAccumulator
    vals.map(i => {
      gm.add(i)
      i
    })
    println(gm.value)
    spark.stop()

  }
}
