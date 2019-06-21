name := "delta-fun"

version := "0.0.1-SNAPSHOT"

scalaVersion := "2.11.12"

libraryDependencies ++= Seq(
  "io.delta" %% "delta-core" % "0.2.0",
  "org.apache.spark" %% "spark-hive" % "2.4.2" % "provided",
  "org.apache.spark" %% "spark-sql" % "2.4.2" % "provided",
  "org.apache.spark" %% "spark-core" % "2.4.2" % "provided",
  "org.apache.spark" %% "spark-catalyst" % "2.4.2" % "provided"
)
