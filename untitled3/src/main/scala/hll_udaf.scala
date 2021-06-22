import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object hll_udaf {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .master("local[8]")
      .appName("SparkKMeans")
      .getOrCreate()
    val sc = spark.sparkContext
    // Reading the csv file => Change the path if needed.
    var df = spark.read.format("csv").option("header", "true").load("./src/main/resources/work_leave.csv")
    // The UDAF
    val gm=new HyperLogLogPlusAgg
    (df.agg(gm(col("ID"))).as("Count").show())
    scala.io.StdIn.readLine()
    // df.agg(approx_count_distinct("Count").as("approx_user_id_count")).show()
  }
}