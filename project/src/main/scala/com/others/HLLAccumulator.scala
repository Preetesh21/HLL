package com.others

import com.cardinality.HLLAccumulator
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.encoders.RowEncoder

/**
 * This object file contains the way of using the HLL Accumulator.
 */


object HLLAccumulator {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Spark")

    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()
    val sc = spark.sparkContext

    var dataframe = spark.read.format("csv")
      .option("header", "true")
      .load("wasbs://aspahdicluster-2020-02-25t09-19-23-989z@aspahdiclustehdistorage.blob.core.windows.net/work_leave.csv")


    val hllAccumulator = new HLLAccumulator
    sc.register(hllAccumulator,"accumulator")

    dataframe=dataframe.map { x => hllAccumulator.add(x.getAs[java.lang.String]("value")); x }(RowEncoder(dataframe.schema))
    dataframe.collect()
    println(hllAccumulator.value)


    println(hllAccumulator.value)
    spark.stop()

  }
}
