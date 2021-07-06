package com.cardinality

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{rand, substring}
import org.apache.spark.sql.types.StringType
import scala.util.Random

/**
 * com.cardinality.Dataset_Generator is responsible for Generating Datasets.
 * There are two possible cases for this: Uniformly Random Distribution and Skew Distribution.
 * For the former one uniformDistribution function is working which takes the number of rows and columns as input
 * and correspondingly generates the dataset. For the latter one I am using a fixed skewed distribution
 * which would always generate numbers in that range and thereby making the entire dataset skewed.
*/

class DatasetGenerator {

  final def skewDistribution[A](distribution: Map[A, Double]): A =
  {
    val probability = scala.util.Random.nextDouble()
    val iterator = distribution.iterator
    var counter = 0.0
    // Generating items based on skew probability distribution
    while (iterator.hasNext) {
      val (item, itemProbability) = iterator.next
      counter += itemProbability

      if (counter >= probability)
        return item // return so that we don't have to search through the whole distribution
    }
    sys.error(f"this should never happen") // needed so it will compile
  }

  def uniformDistribution(rows: Int, columns: Int, spark: SparkSession): DataFrame = {
    import spark.implicits._

    var dataframe = Seq.fill(rows)(Random.nextInt(rows)).toDF()

    if (columns > 1) {
      for (i <- 2 to columns) {
        dataframe = dataframe.withColumn("Col-" + i.toString, substring(rand(), 3, 4).cast("bigint")) // multiple column dataset generation
      }
    }
    dataframe=dataframe.withColumn("value", dataframe("value").cast(StringType)) // typeCasting the first column from int to String
    dataframe // return dataframe
  }

  def generateDataset(category: String, rows: String, columns: String,spark:SparkSession):DataFrame = {
    val form = category

    if (form == "normal") {
      val dataframe = uniformDistribution(rows.toInt, columns.toInt, spark)
      dataframe
    }

    else
    {
      val distribution = Map(21 -> 0.5, 31 -> 0.3, 41 -> 0.2)
      val column = columns.toInt
      val row = rows.toInt
      import spark.implicits._
      var dataframe = Seq.fill(row)(skewDistribution(distribution)).toDF()

      if (column > 1) {
        for (i <- 2 to column) {
          dataframe = dataframe.withColumn("Col-" + i.toString, substring(rand(), 3, 4).cast("bigint"))  // multiple column dataset generation
        }
      }
      dataframe=dataframe.withColumn("value", dataframe("value").cast(StringType)) // typeCasting the first column from int to String
      dataframe // return dataframe
    }
  }
}
