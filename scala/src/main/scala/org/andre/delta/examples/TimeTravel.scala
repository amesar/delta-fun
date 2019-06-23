package org.andre.delta.examples

import org.apache.spark.sql.SparkSession

object TimeTravel {
  val spark = SparkSession.builder.appName("TimeTravel").getOrCreate()
  import spark.implicits._
  println("==== TimeTravel Demo")
  println("spark.version:",spark.version)
  val inDatabricks = System.getenv().get("DATABRICKS_RUNTIME_VERSION") != null
  println("inDatabricks: "+inDatabricks)
             
  def main(args: Array[String]) {
    val dataPath = if (args.size > 0) args(0)  else "delta_fun"
    val database = if (args.size > 1) args(1)  else "delta_fun"
    val table = if (args.size > 2) args(2) else "cats"
    process(dataPath, database, table)
  }

  def process(dataPath: String, database: String, table: String) {
    println("dataPath: "+dataPath)
    println("database: "+database)
    println("table: "+table)

    insert(dataPath, Seq(
      (1, "lion", "africa"),
      (2, "cheetah", "africa"),
      (3, "leopard", "africa")),"overwrite")
    insert(dataPath, Seq(
      (4, "jaguar","south america"),
      (5, "puma","south america"),
      (6, "ocelot","south america")))
    insert(dataPath, Seq(
      (7, "lynx","north america"),
      (8, "bobcat","north america"),
      (9, "catamount","north america")))

    println("== Data Files:")
    val fusePath = dataPath.replace("dbfs:","/dbfs")
    println("fusePath: "+fusePath)
    for (f <- new java.io.File(fusePath).listFiles) {
        println("  "+f)
    }

    println("== All data:")
    val df = spark.read.format("delta").load(dataPath)
    df.sort("id").show()
    df.printSchema

    println("== Data Versions:")
    for (version <- 0 to 2) {
      println(s"Version $version")
      val dfv = spark.read.format("delta").option("versionAsOf", version).load(dataPath)
      dfv.sort("id").show(100,false)
    }

    println("==== SQL")
    spark.sql(s"create database if not exists $database")
    spark.sql(s"use $database")
    spark.sql(s"drop table if exists $table")
    val dfDb = spark.sql("show databases").filter(s"databaseName = '$database'")
    dfDb.show(10,false)

    spark.sql(s"create table $table using delta location '$dataPath'")
    spark.sql(s"describe formatted $table").show(1000,false)

    println("All data:")
    spark.sql(s"select * from $table order by id").show()

    if (inDatabricks) {
      spark.sql(s"describe history $table").show()
      spark.sql(s"describe detail $table").show()
      spark.sql(s"select * from $table order by id").show()
      println("== Data Versions:")
      for (version <- 0 to 2) {
        println(s"Version $version")
        spark.sql(s"select * from $table version as of $version order by id").show()
      }
    }
  }

  def insert(dataPath: String, seq: Seq[(Int,String,String)], mode: String = "append") {
    val df = seq.toDF("id", "name", "region")
    df.coalesce(1).write.mode(mode).format("delta").save(dataPath)
  }
}
