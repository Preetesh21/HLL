package com.others

import com.cardinality.MapAccumulator
import org.apache.spark.sql.SparkSession

object using_map_accumulator {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("Spark")
      .getOrCreate()

    val sc = spark.sparkContext
    val df = spark.read.format("csv")
      .option("header", "true")
      .load("wasbs://aspahdicluster-2020-02-25t09-19-23-989z@aspahdiclustehdistorage.blob.core.windows.net/work_leave.csv")

    var values: List[String] = df.select("ID").collect().map(_ (0)).toList.map(_.toString)

    val gm = new MapAccumulator
    values.map(i => {
      gm.add(i, i.toInt)
      i
    })
    println(gm.value)
    spark.stop()

  }
}
