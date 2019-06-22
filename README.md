# delta-fun
  
Some fun with https://delta.io and https://github.com/delta-io/delta.

## Requirements
* Scala - 2.11.12
* sbt
* Spark - 2.4.2

## Build uber jar

```
sbt "set test in assembly := {}" assembly
```

## Hello World

Based upon https://docs.delta.io/latest/quick-start.html#set-up-apache-spark-with-delta.

### Code

[HelloWorld.scala](src/main/scala/org/andre/delta/examples/HelloWorld.scala)
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

### Run

```
spark-submit --master local[2] --class org.andre.delta.examples.HelloWorld \
  target/scala-2.11/delta-fun-assembly-0.0.1-SNAPSHOT.jar
```

Output
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
