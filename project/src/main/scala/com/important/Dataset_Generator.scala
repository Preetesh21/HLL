package com.important

import org.apache.spark.sql.functions.{rand, substring}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.Random

object Dataset_Generator {

  final def sample[A](dist: Map[A, Double]): A = {
    val p = scala.util.Random.nextDouble()
    val it = dist.iterator
    var accum = 0.0
    while (it.hasNext) {
      val (item, itemProb) = it.next
      accum += itemProb

      if (accum >= p)
        return item // return so that we don't have to search through the whole distribution
    }
    sys.error(f"this should never happen") // needed so it will compile
  }

  def createDummyDataset(rows: Int, columns: Int, spark: SparkSession): DataFrame = {
    import spark.implicits._

    var ds = Seq.fill(rows)(Random.nextInt(rows)).toDF()

    if (columns > 1) {
      for (i <- 2 to columns) {
        ds = ds.withColumn("Col-" + i.toString, substring(rand(), 3, 4).cast("bigint"))
      }
    }
    ds // return ds
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .master("local[8]")
      .appName("SparkKMeans")
      .getOrCreate()
    val form = args(0)

    if (form == "normal") {
      val ds = createDummyDataset(args(1).toInt, args(2).toInt, spark)
      ds.repartition(1).write.option("header", true).format("csv").save("./src/main/resources/data")
    }

    else {
      val dist = Map(21 -> 0.5, 31 -> 0.3, 41 -> 0.2)
      val columns = args(2).toInt
      val rows = args(1).toInt
      import spark.implicits._
      var ds = Seq.fill(rows)(sample(dist)).toDF()

      if (columns > 1) {
        for (i <- 2 to columns) {
          ds = ds.withColumn("Col-" + i.toString, substring(rand(), 3, 4).cast("bigint"))
        }
      }
      ds.repartition(1).write.option("header", true).format("csv").save("./src/main/resources/data")

    }
    spark.stop()
  }
}
