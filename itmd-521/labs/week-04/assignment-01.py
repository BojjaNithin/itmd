from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Create SparkSession
spark = SparkSession.builder \
    .appName("Divvy Trips Analysis") \
    .getOrCreate()

# Define schema
schema = StructType([
    StructField("trip_id", IntegerType(), True),
    StructField("starttime", StringType(), True),
    StructField("stoptime", StringType(), True),
    StructField("bikeid", IntegerType(), True),
    StructField("tripduration", IntegerType(), True),
    StructField("from_station_id", IntegerType(), True),
    StructField("from_station_name", StringType(), True),
    StructField("to_station_id", IntegerType(), True),
    StructField("to_station_name", StringType(), True),
    StructField("usertype", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("birthyear", StringType(), True)
])

# Read CSV with inferred schema
df_inferred = spark.read.csv("Divvy_Trips_2015-Q1.csv", header=True, inferSchema=True)
print("Inferred Schema:")
df_inferred.printSchema()
print("Number of records:", df_inferred.count())

# Read CSV with programmatically attached schema
df_programmatic = spark.read.csv("Divvy_Trips_2015-Q1.csv", header=True, schema=schema)
print("\nProgrammatic Schema:")
df_programmatic.printSchema()
print("Number of records:", df_programmatic.count())

# Read CSV with schema via DDL
df_ddl = spark.read.option("header", "true").csv("Divvy_Trips_2015-Q1.csv")
df_ddl.createOrReplaceTempView("divvy_trips")
df_ddl_with_schema = spark.sql("SELECT * FROM divvy_trips")
print("\nSchema via DDL:")
df_ddl_with_schema.printSchema()
print("Number of records:", df_ddl_with_schema.count())

# Select Gender based on last name and group by station to
selected_gender = df_ddl_with_schema.selectExpr("CASE WHEN substring(gender, 1, 1) >= 'A' AND substring(gender, 1, 1) <= 'K' THEN 'Female' ELSE 'Male' END AS selected_gender", "to_station_name").groupBy("to_station_name").count()
selected_gender.show(10)

# Stop SparkSession
spark.stop()
