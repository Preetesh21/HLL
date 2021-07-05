package com.others

import com.cardinality.HyperLogLog_defined_by_me
import org.apache.spark.sql.SparkSession

object using_hll_defined_by_me extends App {
  val hll = new HyperLogLog_defined_by_me
  val spark = SparkSession
    .builder
    .appName("SparkKMeans")
    .getOrCreate()

  //var df = spark.read.format("csv").option("header", "true").load("https://aspahdiclustehdistorage.blob.core.windows.net/aspahdicluster-2020-02-25t09-19-23-989z/work_leave.csv").toDF()

  val df = spark.read.format("csv")
    .option("header", "true")
    .load("wasbs://aspahdicluster-2020-02-25t09-19-23-989z@aspahdiclustehdistorage.blob.core.windows.net/work_leave.csv")
  var vals: List[Int] = df.select("ID").collect().map(_ (0)).toList.map(_.toString.toInt)

  // custom function evaluate type
  vals.map(i => {
    hll.addValue(i)
    i
  })
  //    val values = (1 to 2000000).map(i => {
  //      val value = Random.nextInt(16777215)
  //      hll.addValue(value)
  //      value
  //    })


  // Actual count of distinct values
  val actualCount = vals.toSet.size

  // Estimated count of distinct values from hyperloglog
  val estimatedCount = hll.getCount

  println(actualCount)
  println(estimatedCount)
  spark.stop()
}
