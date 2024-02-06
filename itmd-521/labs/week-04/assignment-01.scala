import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object DivvyTripsAnalysis {
  def main(args: Array[String]): Unit = {
    // Create SparkSession
    val spark = SparkSession.builder()
      .appName("Divvy Trips Analysis")
      .getOrCreate()

    // Define schema
    val schema = StructType(Array(
      StructField("trip_id", IntegerType, nullable = true),
      StructField("starttime", StringType, nullable = true),
      StructField("stoptime", StringType, nullable = true),
      StructField("bikeid", IntegerType, nullable = true),
      StructField("tripduration", IntegerType, nullable = true),
      StructField("from_station_id", IntegerType, nullable = true),
      StructField("from_station_name", StringType, nullable = true),
      StructField("to_station_id", IntegerType, nullable = true),
      StructField("to_station_name", StringType, nullable = true),
      StructField("usertype", StringType, nullable = true),
      StructField("gender", StringType, nullable = true),
      StructField("birthyear", StringType, nullable = true)
    ))

    // Read CSV with inferred schema
    val dfInferred = spark.read.option("header", "true").csv("Divvy_Trips_2015-Q1.csv")
    println("Inferred Schema:")
    dfInferred.printSchema()
    println("Number of records:", dfInferred.count())

    // Read CSV with programmatically attached schema
    val dfProgrammatic = spark.read.option("header", "true").schema(schema).csv("Divvy_Trips_2015-Q1.csv")
    println("\nProgrammatic Schema:")
    dfProgrammatic.printSchema()
    println("Number of records:", dfProgrammatic.count())

    // Read CSV with schema via DDL
    val dfDDL = spark.read.option("header", "true").csv("Divvy_Trips_2015-Q1.csv")
    dfDDL.createOrReplaceTempView("divvy_trips")
    val dfDDLWithSchema = spark.sql("SELECT * FROM divvy_trips")
    println("\nSchema via DDL:")
    dfDDLWithSchema.printSchema()
    println("Number of records:", dfDDLWithSchema.count())

    // Select Gender based on last name and group by station to
    val selectedGender = dfDDLWithSchema.selectExpr("CASE WHEN substring(gender, 1, 1) >= 'A' AND substring(gender, 1, 1) <= 'K' THEN 'Female' ELSE 'Male' END AS selected_gender", "to_station_name").groupBy("to_station_name").count()
    selectedGender.show(10)

    // Stop SparkSession
    spark.stop()
  }
}
