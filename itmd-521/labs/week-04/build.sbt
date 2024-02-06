name := "divvy-trips-analysis"

version := "1.0"

scalaVersion := "2.12.10"

// Define dependencies
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.5.0",
  "org.apache.spark" %% "spark-sql" % "3.5.0"
)

// Define main class
mainClass in Compile := Some("DivvyTripsAnalysis")

