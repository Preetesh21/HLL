package com.cardinality

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.functions.col

/**
 *  com.cardinality.Main_Implementation is the main object which intakes arguments from the user
 *  and depending on those implements one of the 4 techniques stated below.
 *  The arguments it takes includes the to be generated dataset's number of rows and columns and it's type such as
 *  Skew or Normal. Then it takes the technique to be implemented and then if any of the techniques require an argument.
 *  To check the accuracy of the techniques one of the in built function CountDistinct could be used which has been commented in
 *  the below code. This object would return the count of the unique items present in the list.
 */


object MainImplementation {
  def main(args: Array[String]): Unit = {

    // Dataset Generator related arguments
    val rows = args(0)
    val cols = args(1)
    val category = args(2)

    // The technique related argument
    var Technique: Int = 0
    print(" The technique that could be deployed \n" +
      "[1]-> HLL integrated with Accumulator Datastructures  \n" +
      "[2]-> HLL integrated with UDAF datastructures  \n" +
      "[3]-> HLL provided by spark  \n" +
      "[4]-> Map Accumulator Solution \n"+args(1).toInt + "\n")

    Technique = args(3).toInt

    val spark = SparkSession
      .builder
      .appName("Spark_Main")
      .getOrCreate()

    val sc = spark.sparkContext

    val generator = new DatasetGenerator
    var dataframe=generator.generateDataset(category,rows,cols, spark)

//    import org.apache.spark.sql.functions.{col, countDistinct}
//    df.select(df.columns.slice(0,1).map(c => countDistinct(col(c)).alias(c)): _*).show()
//    var df = spark.read.format("csv").option("header", "true").load(dataPath)

    Technique match {

      case 1 =>
        val accumulator = new HLLAccumulator
        sc.register(accumulator,"accumulator")

        dataframe=dataframe.map { x => accumulator.add(x.getAs[java.lang.String]("value")); x }(RowEncoder(dataframe.schema))
        dataframe.collect()

        //println(acc.value)

      case 2 =>
        val hll_agg = new HLLAggregator

        dataframe.agg(hll_agg(col("value"))).as("Count").show()

      case 3 =>
        val expressions = dataframe.columns.slice(0,1).map(_ -> "approx_count_distinct").toMap
        dataframe.agg(expressions).show()

      case _ =>
        var param=true
        if(args.length==5) {
          param = args(4).toBoolean
        }

        val mapAccumulator = new MapAccumulator(param)
        sc.register(mapAccumulator,"accumulator")

        dataframe=dataframe.map { x => mapAccumulator.add(x.getAs[java.lang.String]("value"),1); x }(RowEncoder(dataframe.schema))
        dataframe.collect()

        //println(acc.value)
    }
    spark.stop()
  }
}



