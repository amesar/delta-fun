package org.andre.delta.sample

import org.apache.spark.sql.SparkSession

object HelloWorld {
  val spark = SparkSession.builder.appName("HelloWorld").getOrCreate()
             
  def main(args: Array[String]) {
    val dataPath = "delta-table"
    println("dataPath: "+dataPath)
    val data = spark.range(0, 5)
    data.write.format("delta").save(dataPath)
    val df = spark.read.format("delta").load(dataPath)
    println("Data:\n")
    df.show()
    println("Schema:\n")
    df.printSchema()
  } 
}
