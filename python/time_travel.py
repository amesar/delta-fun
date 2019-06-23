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
table = "cats"

def insert(path, data, mode="append"):
    df = spark.createDataFrame(data, ["id","name","region"])
    df.coalesce(1).write.mode(mode).format("delta").save(path)

def init():
    if in_databricks:
        user = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().get("user").get()
        user = user.split("@")[0].replace(".","_")
        database = "{}_delta_fun".format(user)
        dataPath = "dbfs:/tmp/{}/delta_fun/table_cats".format(user)
    else:
        database = "delta_fun"
        dataPath = "delta_fun"
    return (database,dataPath)


def main():
    database,dataPath = init()
    print("database:",database)
    print("table:",table)
    print("dataPath:",dataPath)

    insert(dataPath, [
        (1, "lion","africa"),
        (2, "cheetah","africa"),
        (3, "leopard","africa")], "overwrite")
    insert(dataPath, [
        (4, "jaguar","south america"),
        (5, "puma","south america"),
        (6, "ocelot","south america")])
    insert(dataPath, [
        (7, "lynx","north america"),
        (8, "bobcat","north america"),
        (9, "catamount","north america")])

    print("Data Files:")
    files = dbutils.fs.ls(dataPath) if in_databricks else os.listdir(dataPath)
    for f in files:
        print("  ",f)

    print("All data:")
    df = spark.read.format("delta").load(dataPath)
    df.sort("id").show()

    print("Data Versions:")
    for v in range(0,3):
        print("Version:",v)
        df = spark.read.format("delta").option("versionAsOf", str(v)).load(dataPath)
        df.sort("id").show(100,False)

    print("==== SQL")
    spark.sql("create database if not exists {}".format(database))
    spark.sql("use {}".format(database))
    spark.sql("drop table if exists {}".format(table))
    df = spark.sql("show databases").filter("databaseName = '{}'".format(database))

    spark.sql("create table {} using delta location '{}'".format(table,dataPath))
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

if __name__ == "__main__":
    main()
