package com.others

import com.cardinality.MapAccumulator
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.encoders.RowEncoder

/**
 * This object file contains the way of using the Map Accumulator defined by me.
 */

object MapAccumulator {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .appName("Spark")
      .getOrCreate()
    val sc = spark.sparkContext

    var dataframe = spark.read.format("csv")
      .option("header", "true")
      .load("wasbs://aspahdicluster-2020-02-25t09-19-23-989z@aspahdiclustehdistorage.blob.core.windows.net/work_leave.csv")

    val mapAccumulator = new MapAccumulator(true)
    sc.register(mapAccumulator,"accumulator")

    dataframe=dataframe.map { x => mapAccumulator.add(x.getAs[java.lang.String]("value"),1); x }(RowEncoder(dataframe.schema))
    dataframe.collect()
    println(mapAccumulator.value)

    spark.stop()
  }
}
