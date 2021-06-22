import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object udf_email_validator {
  // Function to check the email is valid or not
  def isValid(email: String): Boolean = {
    if("""^[a-zA-Z0-9\.!#$%&'*+/=?^_`{|}~-]+@[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?(?:\.[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)*$""".r.findFirstIn(email).isEmpty) {
      false
    }
    else {
      true
    }
  }

  // UDF to validate the email.
  def cleanEmail = (email: String) => {
    if(isValid(email)) {
      "Valid"
    }
    else {
      "Invalid"
    }
  }
  def main(args: Array[String]): Unit = {

    // Creating Spark Session
    val spark = SparkSession
      .builder
      .master("local[8]")
      .appName("SparkKMeans")
      .getOrCreate()
    val sc=spark.sparkContext
    // Reading the csv file => Change the path if needed.
    val inputDF=spark.read.format("csv").load("./src/main/resources/sample.csv").toDF("col1", "col2", "col3")

    // Registering the UDF
    val emailUDF = spark.udf.register("cleanEmail",cleanEmail)

    // Performing the UDF operation on the Dataframe => First classifying the emails as Valid or Invalid based on the criteria given
    // and then filtering out the invalid ones and saving the valid ones to csv file.
    var tempDF=inputDF.withColumn("Valid", emailUDF(col("col2")))
    tempDF=tempDF.filter(tempDF("Valid")==="Valid")
    //tempDF.show()
    // Filtering valid emails which dont have domains of Microsoft.
    tempDF=tempDF.filter(tempDF("col2").contains("microsoft.com")|| tempDF("col2").contains("outlook.com")|| tempDF("col2").contains("hotmail.com"))
    tempDF=tempDF.drop("Valid")
    var accum = sc.accumulator(0)
    tempDF.foreach(Row=>accum+=1)
    println("The number of rows that are valid are:"+accum)
    tempDF.write.format("csv").save("./src/main/resources/datacsv2")
    scala.io.StdIn.readLine()
  }


}
