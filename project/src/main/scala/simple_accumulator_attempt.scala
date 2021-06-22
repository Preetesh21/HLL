import org.apache.spark.sql.SparkSession

object accumulator {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .master("local[8]")
      .appName("SparkKMeans")
      .getOrCreate()
    val sc=spark.sparkContext
    // Reading the csv file => Change the path if needed.
    val df = spark.read.format("csv").option("header", "true").load("./src/main/resources/work_leave.csv")
    // The accumulator
    var accum = sc.accumulator(0)
    val df2=df.groupBy("ID").count().foreach(Row=>accum+=1)
    println(accum)
  }
}
