# delta-fun
  
Some fun with https://delta.io and https://github.com/delta-io/delta.

## Overview

**Examples**
* Hello World - Scala and Python (script and Jupyter notebook).
* Time Travel - Basic Delta time travel. Scala and Python (script and Jupyter notebook).

**Notes**
* Examples run with OSS Delta on laptop.
* All code is instrumented to run on Databricks as well.
* Notebooks can run as Jupyter of Databricks notebooks.


## Requirements
* Spark 2.4.2
* Python 3.6
* Scala:
  * Scala 2.12.8
  * sbt

For full details see: https://docs.delta.io/latest/quick-start.html#set-up-apache-spark-with-delta.


## Setup

### Scala

For local use, build the uber jar.
```
cd scala
sbt "set test in assembly := {}" assembly
```

For use in Databricks cluster, build the slim jar.
```
cd scala
sbt package
```

### Python Jupyter
Before launching Jupyter set the following environment variable.
```
export PYSPARK_SUBMIT_ARGS="--packages io.delta:delta-core_2.12:0.2.0 pyspark-shell"
```

## Hello World

Based upon https://docs.delta.io/latest/quick-start.html#set-up-apache-spark-with-delta.

### Code

#### Scala

[HelloWorld.scala](scala/src/main/scala/org/andre/delta/examples/HelloWorld.scala)
```
package org.andre
import org.apache.spark.sql.SparkSession

object HelloWorld {
  val spark = SparkSession.builder.appName("HelloWorld").getOrCreate()

  def main(args: Array[String]) {
    val dataPath = "delta-table"
    println("dataPath: "+dataPath)
    val data = spark.range(0, 5)
    data.write.format("delta").save(dataPath)
    val df = spark.read.format("delta").load(dataPath)
    df.show()
    df.printSchema()
  }
}
```

#### Python

[hello_world.py](python/hello_world.py)
or [hello_world.ipynb](python/hello_world.ipynb).
```
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("HelloWorld").getOrCreate()

dataPath = "delta-table"
print("dataPath: "+dataPath)
data = spark.range(0, 5)
data.write.format("delta").save(dataPath)
df = spark.read.format("delta").load(dataPath)
print("Data:\n")
df.show()
print("Schema:\n")
df.printSchema()
```


### Run

#### Scala
```
cd scala
spark-submit --master local[2] --class org.andre.delta.examples.HelloWorld \
  target/scala-2.11/delta-fun-assembly-0.0.1-SNAPSHOT.jar
```

#### Python
```
cd python
spark-submit --master local[2] \
  --packages io.delta:delta-core_2.12:0.2.0 \
  hello_world.py
```

#### Output
```
dataPath: delta-table

Data:

+---+
| id|
+---+
|  2|
|  3|
|  4|
|  0|
|  1|
+---+

Schema:

root
 |-- id: long (nullable = true)
```

### Check delta table files

ls delta-table
```

.part-00000-f06b6eb0-9394-4f12-bb83-e61fef260544-c000.snappy.parquet.crc
.part-00001-b50b30e6-0297-4e3c-a05e-538753208a0e-c000.snappy.parquet.crc
_delta_log/
part-00000-f06b6eb0-9394-4f12-bb83-e61fef260544-c000.snappy.parquet
part-00001-b50b30e6-0297-4e3c-a05e-538753208a0e-c000.snappy.parquet
```

ls delta-table/\_delta_log
```
..00000000000000000000.json.1c577716-2635-4da5-a372-40cf3307d90c.tmp.crc
00000000000000000000.json
```

### Delta transaction log file

[delta-table\_delta_log/00000000000000000000.json](sample/formatted_00000000000000000000.json)
```
{
  "commitInfo": {
    "timestamp": 1561071682645,
    "operation": "WRITE",
    "operationParameters": {
      "mode": "ErrorIfExists",
      "partitionBy": "[]"
    },
    "isBlindAppend": true
  }
}
{
  "protocol": {
    "minReaderVersion": 1,
    "minWriterVersion": 2
  }
}
{
  "metaData": {
    "id": "2ec1b867-59da-4fcb-8398-51c4f894cf06",
    "format": {
      "provider": "parquet",
      "options": {}
    },
    "schemaString": "{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}}]}",
    "partitionColumns": [],
    "configuration": {},
    "createdTime": 1561071681950
  }
}
{
  "add": {
    "path": "part-00000-3283a0bd-655e-4a7e-988f-afae997d8095-c000.snappy.parquet",
    "partitionValues": {},
    "size": 431,
    "modificationTime": 1561071682000,
    "dataChange": true
  }
}
. . .
```

## Time Travel

Insert three batches of data and query each version with time travel.
```
spark.read.format("delta").option("versionAsOf", "1")).load(dataPath).show()
```

### Scala

Source: [TimeTravel.scala](scala/src/main/scala/org/andre/delta/examples/TimeTravel.scala).

```
cd scala
spark-submit --master local[2] --class org.andre.delta.examples.TimeTravel \
  target/scala-2.11/delta-fun-assembly-0.0.1-SNAPSHOT.jar
```

#### Run in Databricks
Run with [run_submit.json](scala/run_submit.json) run spec.
```
databricks runs submit --json-file run_submit.json
```

First push the _slim_ jar to DBFS.
```
databricks fs cp \
  target/scala-2.11/delta-fun_2.11-0.0.1-SNAPSHOT.jar \
  dbfs:/delta_fun/jars/delta-fun-assembly-0.0.1-SNAPSHOT.jar \
   --overwrite
```

### Python

Source: [time_travel.py.scala](python/time_travel.py) or [time_travel.ipynb](python/time_travel.ipynb).
```
cd python
spark-submit --master local[2] \
  --packages io.delta:delta-core_2.12:0.2.0 \
  time_travel.py
```

#### Run in Databricks
Run with [run_submit.json](python/run_submit.json) run spec.
```
databricks runs submit --json-file run_submit.json
```

First push the Python script to DBFS.
```
databricks fs cp time_travel.py \
  dbfs:/delta_fun/python/time_travel.py \
   --overwrite
```
