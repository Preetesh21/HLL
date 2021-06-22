import org.apache.spark.sql.SparkSession
object hll_defined extends App {
  val hll = new HyperLogLog
  val spark = SparkSession
    .builder
    .master("local[8]")
    .appName("SparkKMeans")
    .getOrCreate()

  var df = spark.read.format("csv").option("header", "true").load("./src/main/resources/work_leave.csv").toDF()
  var vals:List[Int] = df.select("ID").collect().map(_(0)).toList.map(_.toString.toInt)

  // custom function evaluate type
  vals.map(i=>{
    hll.addValue(i)
    i
  })
  //    val values = (1 to 2000000).map(i => {
  //      val value = Random.nextInt(16777215)
  //      hll.addValue(value)
  //      value
  //    })


  // Actual count of distinct values
  val actualCount = vals.toSet.size

  // Estimated count of distinct values from hyperloglog
  val estimatedCount = hll.getCount

  println(actualCount)
  println(estimatedCount)
}

