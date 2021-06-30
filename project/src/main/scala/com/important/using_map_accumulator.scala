package com.important
import org.apache.spark.sql.SparkSession

object using_map_accumulator{
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .master("local[8]")
      .appName("SparkKMeans")
      .getOrCreate()
    var df = spark.read.format("csv").option("header", "true").load("./src/main/resources/work_leave.csv")
    val sc = spark.sparkContext
    var values:List[String] = df.select("ID").collect().map(_(0)).toList.map(_.toString)

    val gm = new MapAccumulator
    values.map(i=>{
      gm.add(i,i.toInt)
      i
    })
    println(gm.value.size)
    spark.stop()

  }
}
