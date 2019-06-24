"""
Delta Time Travel Demo
 - Demonstrate Delta time travel features
 - Works on OSS Delta on laptop and in Databricks cluster
"""

import sys, os
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("TimeTravel").getOrCreate()
print("==== TimeTravel Demo")
print("Spark version:",spark.version)
print("Python version:",sys.version)

in_databricks = 'DATABRICKS_RUNTIME_VERSION' in os.environ
print("in_databricks:",in_databricks)

def insert(path, data, mode="append"):
    df = spark.createDataFrame(data, ["id","name","region"])
    df.coalesce(1).write.mode(mode).format("delta").save(path)

def main(data_path, database, table):
    print("data_path:",data_path)
    print("database:",database)
    print("table:",table)

    insert(data_path, [
        (1, "lion","africa"),
        (2, "cheetah","africa"),
        (3, "leopard","africa")], "overwrite")
    insert(data_path, [
        (4, "jaguar","south america"),
        (5, "puma","south america"),
        (6, "ocelot","south america")])
    insert(data_path, [
        (7, "lynx","north america"),
        (8, "bobcat","north america"),
        (9, "catamount","north america")])

    print("Data Files:")
    files = dbutils.fs.ls(data_path) if in_databricks else os.listdir(data_path)
    for f in files:
        print("  ",f)

    print("All data:")
    df = spark.read.format("delta").load(data_path)
    df.sort("id").show()

    print("Data Versions:")
    for v in range(0,3):
        print("Version:",v)
        df = spark.read.format("delta").option("versionAsOf", str(v)).load(data_path)
        df.sort("id").show(100,False)

    print("==== SQL")
    spark.sql("create database if not exists {}".format(database))
    spark.sql("use {}".format(database))
    spark.sql("drop table if exists {}".format(table))
    df = spark.sql("show databases").filter("databaseName = '{}'".format(database))

    spark.sql("create table {} using delta location '{}'".format(table,data_path))
    spark.sql("describe formatted {}".format(table)).show(1000,False)

    print("All data:")
    spark.sql("select * from {} order by id".format(table)).show()

    if in_databricks: 
        spark.sql("describe history {}".format(table)).show()
        spark.sql("describe detail {}".format(table)).show()
        print("Data Versions:")
        for v in range(0,3):
            print("Version:",v)
            spark.sql("select * from {} version as of {} order by id".format(table,v)).show()

    print("Everything ran OK")

if __name__ == "__main__":
    data_path = sys.argv[1] if len(sys.argv) > 1 else "delta_fun"
    database = sys.argv[2] if len(sys.argv) > 2 else "delta_fun"
    table = sys.argv[3] if len(sys.argv) > 3 else "cats"
    main(data_path,database,table)
