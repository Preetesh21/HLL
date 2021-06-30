package com.important

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object mains {
  def main(args: Array[String]): Unit = {
    print("Enter the path\n")
    val dataPath = scala.io.StdIn.readLine()

    var Technique: Int = 0
    print("Enter the technique you want to be deployed " +
      "[1]-> HLL integrated with Accumulator Datastructures  " +
      "[2]-> HLL integrated with UDAF datastructures  " +
      "[3]-> HLL provided by spark  " +
      "[4]-> Map Accumulator Solution \n")
    while(Technique != 1 && Technique != 2 && Technique != 3 && Technique!= 4){
      Technique = scala.io.StdIn.readInt()
    }

    val spark = SparkSession
      .builder
      .master("local[8]")
      .appName("Spark_Main")
      .getOrCreate()
    val sc = spark.sparkContext

    val df = spark.read.format("csv").option("header", "true").load(dataPath)

    Technique match {

      case 1 =>
        val values: List[String] = df.select("ID").collect().map(_ (0)).toList.map(_.toString)
        val hll_acc = new HLLAccumulator
        values.map(i => {
          hll_acc.add(i)
          i
        })
        println(hll_acc.value)

      case 2 =>
        val hll_agg = new HyperLogLogPlusAggregator
        df.agg(hll_agg(col("ID"))).as("Count").show()

      case 3 =>
        val expressions = df.columns.slice(0,1).map(_ -> "approx_count_distinct").toMap
        df.agg(expressions).show()

      case _ =>
        val values:List[String] = df.select("ID").collect().map(_(0)).toList.map(_.toString)

        val map_acc = new MapAccumulator
        values.map(i=>{
          map_acc.add(i,i.toInt)
          i
        })
        println(map_acc.value.size)
    }
    spark.stop()
  }
}


