package com.cardinality

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.functions.col

object mains {
  def main(args: Array[String]): Unit = {
    print("The path"+ args(0)+"\n")
    val rows = args(0)
    val cols = args(1)
    val category = args(2)

    var Technique: Int = 0
    print(" The technique that could be deployed \n" +
      "[1]-> HLL integrated with Accumulator Datastructures  \n" +
      "[2]-> HLL integrated with UDAF datastructures  \n" +
      "[3]-> HLL provided by spark  \n" +
      "[4]-> Map Accumulator Solution \n"+args(1).toInt + "\n")
    while(Technique != 1 && Technique != 2 && Technique != 3 && Technique!= 4){
      Technique = args(3).toInt
    }

    val spark = SparkSession
      .builder
      .appName("Spark_Main")
      .getOrCreate()
    val sc = spark.sparkContext

    var df=Dataset_Generator.create(category,rows,cols, spark)

//    import org.apache.spark.sql.functions.{col, countDistinct}
//    df.select(df.columns.slice(0,1).map(c => countDistinct(col(c)).alias(c)): _*).show()
    //var df = spark.read.format("csv").option("header", "true").load(dataPath)

    Technique match {

      case 1 =>
        val acc = new HLLAccumulator
        sc.register(acc,"accumulator")
        df=df.map { x => acc.add(x.getAs[java.lang.String]("value")); x }(RowEncoder(df.schema))
        df.collect()
        println(acc.value)

      case 2 =>
        val hll_agg = new HyperLogLogPlusAggregator

        df.agg(hll_agg(col("value"))).as("Count").show()

      case 3 =>
        val expressions = df.columns.slice(0,1).map(_ -> "approx_count_distinct").toMap
        df.agg(expressions).show()

      case _ =>
        var param=true;
        if(args.length==5) {
          param = args(4).toBoolean
        }
        val acc = new MapAccumulator(param)
        sc.register(acc,"accumulator")
        df=df.map { x => acc.add(x.getAs[java.lang.String]("value"),1); x }(RowEncoder(df.schema))
        df.collect()
        println(acc.value)
    }
    spark.stop()
  }
}



