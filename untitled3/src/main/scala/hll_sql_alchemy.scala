import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.approx_count_distinct

object hll_sql_alchemy {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .master("local[8]")
      .appName("SparkKMeans")
      .getOrCreate()
    val sc=spark.sparkContext
    // Reading the csv file => Change the path if needed.
    val df = spark.read.format("csv").option("header", "true").load("./src/main/resources/work_leave.csv")
    // Using the approx_count method based on the HLL algorithm.
    df.agg(approx_count_distinct("ID").as("approx_user_id_count")).show()
  }
}
