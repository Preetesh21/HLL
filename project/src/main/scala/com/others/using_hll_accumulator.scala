package com.others

import com.cardinality.HLLAccumulator
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object using_hll_accumulator {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Spark")

    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext

    //var df = spark.read.format("csv").option("header", "true").load("https://aspahdiclustehdistorage.blob.cor020-02-25t09-19-23-989z/work_leave.csv").toDF()
    val df = spark.read.format("csv")
      .option("header", "true")
      .load("wasbs://aspahdicluster-2020-02-25t09-19-23-989z@aspahdiclustehdistorage.blob.core.windows.net/work_leave.csv")
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
