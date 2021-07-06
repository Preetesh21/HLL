package com.others

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.encoders.RowEncoder

import scala.util.Random

/**
 * This object file contains the way of using the HLL CLass defined by me. Here the current active code is capable of outputting
 * for a datset of specified size. The commented part shows the code for dataset input and its size calculation.
 */

object ObjectHLL extends App {

  val hll = new HLL
  val spark = SparkSession
    .builder
    .appName("SparkKMeans")
    .getOrCreate()

//  var dataframe = spark.read.format("csv")
//    .option("header", "true")
//    .load("wasbs://aspahdicluster-2020-02-25t09-19-23-989z@aspahdiclustehdistorage.blob.core.windows.net/work_leave.csv")
//
//  // custom function evaluate type
//  dataframe=dataframe.map { x => hll.addValue(x.getAs("ID")); x }(RowEncoder(dataframe.schema))
//  dataframe.collect()
//
//  var values: List[Int] = dataframe.select("ID").collect().map(_ (0)).toList.map(_.toString.toInt)
//  values.map(i => {
//    hll.addValue(i)
//    i
//  })

  val index=args(0).toInt

  val values = (1 to index).map(i => {
        val value = Random.nextInt(16777215)
        hll.addValue(value)
        value
  })


  // Actual count of distinct values
  val actualCount = values.toSet.size

  // Estimated count of distinct values from hyperloglog
  val estimatedCount = hll.getCount

  println(actualCount)
  println(estimatedCount)

  spark.stop()
}
