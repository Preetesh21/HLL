package com.others

import com.cardinality.HLLAggregator
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

/**
 * This object file contains the way of using the HLL_UDAF defined by me.
 */

object HLLUDAF {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .appName("SparkKMeans")
      .getOrCreate()

    // Reading the csv file => Change the path if needed.
    val df = spark.read.format("csv")
      .option("header", "true")
      .load("wasbs://aspahdicluster-2020-02-25t09-19-23-989z@aspahdiclustehdistorage.blob.core.windows.net/work_leave.csv")

    val gm = new HLLAggregator
    df.agg(gm(col("ID"))).as("Count").show()

    spark.stop()
    }
}
