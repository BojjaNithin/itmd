import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.types._

object Assignment01 {
  def main(args: Array[String]): Unit = {

    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("Assignment01")
      .config("spark.master", "local")
      .getOrCreate()

    // Define schema for CSV file
    val schema = new StructType()
      .add("trip_id", LongType)
      .add("starttime", TimestampType)
      .add("stoptime", TimestampType)
      .add("bikeid", IntegerType)
      .add("tripduration", IntegerType)
      .add("from_station_id", IntegerType)
      .add("from_station_name", StringType)
      .add("to_station_id", IntegerType)
      .add("to_station_name", StringType)
      .add("usertype", StringType)
      .add("gender", StringType)
      .add("birthyear", IntegerType)

    // Read CSV file with inferred schema
    val df1 = readCSV(spark, "Divvy_Trips_2015-Q1.csv")
    println("DataFrame 1 Schema:")
    df1.printSchema()
    println("Count: " + df1.count())

    // Read CSV file with programmatically defined schema
    val df2 = readCSV(spark, "Divvy_Trips_2015-Q1.csv", schema)
    println("DataFrame 2 Schema:")
    df2.printSchema()
    println("Count: " + df2.count())

    // Read CSV file with DDL-defined schema
    val ddlSchema = "trip_id LONG, starttime TIMESTAMP, stoptime TIMESTAMP, bikeid INT, tripduration INT, from_station_id INT, from_station_name STRING, to_station_id INT, to_station_name STRING, usertype STRING, gender STRING, birthyear INT"
    val df3 = readCSV(spark, "Divvy_Trips_2015-Q1.csv", StructType.fromDDL(ddlSchema))

    println("DataFrame 3 Schema:")
    df3.printSchema()
    println("Count: " + df3.count())

    // Stop the SparkSession
    spark.stop()
  }

  // Function to read CSV with inferred, programmatically defined, or DDL-defined schema
  def readCSV(spark: SparkSession, path: String, schema: StructType = null, ddlSchema: String = null): DataFrame = {
    var df: DataFrame = null
    if (ddlSchema != null) {
      df = spark.read.option("header", "true").option("delimiter", ",").schema(ddlSchema).csv(path)
    } else if (schema != null) {
      df = spark.read.option("header", "true").schema(schema).csv(path)
    } else {
      df = spark.read.option("header", "true").csv(path)
    }
    df
  }
}
