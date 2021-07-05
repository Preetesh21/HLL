package com.cardinality

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{rand, substring}
import org.apache.spark.sql.types.StringType

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
    ds=ds.withColumn("value", ds("value").cast(StringType))
    ds // return ds
  }

  def create(category: String, rows: String, cols: String,spark:SparkSession):DataFrame = {
    val form = category

    if (form == "normal") {
      val ds = createDummyDataset(rows.toInt, cols.toInt, spark)
      ds
      //ds.repartition(1).write.option("header", true).format("csv").save("./src/main/resources/data")
    }

    else {
      val dist = Map(21 -> 0.5, 31 -> 0.3, 41 -> 0.2)
      val columns = cols.toInt
      val row = rows.toInt
      import spark.implicits._
      var ds = Seq.fill(row)(sample(dist)).toDF()

      if (columns > 1) {
        for (i <- 2 to columns) {
          ds = ds.withColumn("Col-" + i.toString, substring(rand(), 3, 4).cast("bigint"))
        }
      }
      ds=ds.withColumn("value", ds("value").cast(StringType))
      ds
      //ds.repartition(1).write.option("header",true).format("csv").save("./src/main/resources/data")

    }
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .master("local[8]")
      .appName("SparkKMeans")
      .getOrCreate()
  }
}
