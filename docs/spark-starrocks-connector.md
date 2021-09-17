<!--
Modifications Copyright 2021 StarRocks Limited.

Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Spark StarRocks Connector

Spark StarRocks Connector can support reading data stored in StarRocks through Spark.

- The current version only supports reading data from `StarRocks`.
- You can map the `StarRocks` table to `DataFrame` or `RDD`, it is recommended to use `DataFrame`.
- Support the completion of data filtering on the `StarRocks` side to reduce the amount of data transmission.

## Version Compatibility

| Connector | Spark | StarRocks | Java | Scala |
| --------- | ----- | --------- | ---- | ----- |
| 1.0.0     | 2.x   | 1.18+     | 8    | 2.11  |


## Build and Install

Execute following command

```bash
sh build.sh
```

After successful compilation, the file `spark-starrocks-connector-1.0.0-SNAPSHOT.jar` will be generated in the `output/` directory. Copy this file to `ClassPath` in `Spark` to use `Spark StarRocks Connector`. For example, `Spark` running in `Local` mode, put this file in the `jars/` folder. `Spark` running in `Yarn` cluster mode, put this file in the pre-deployment package.

## Example

### SQL

```sql
CREATE TEMPORARY VIEW spark_starrocks
USING starrocks
OPTIONS(
  "table.identifier" = "$YOUR_STARROCKS_DATABASE_NAME.$YOUR_STARROCKS_TABLE_NAME",
  "fenodes" = "$YOUR_STARROCKS_FE_HOSTNAME:$YOUR_STARROCKS_FE_RESFUL_PORT",
  "user" = "$YOUR_STARROCKS_USERNAME",
  "password" = "$YOUR_STARROCKS_PASSWORD"
);

SELECT * FROM spark_starrocks;
```

### DataFrame

```scala
val starrocksSparkDF = spark.read.format("starrocks")
  .option("starrocks.table.identifier", "$YOUR_STARROCKS_DATABASE_NAME.$YOUR_STARROCKS_TABLE_NAME")
  .option("starrocks.fenodes", "$YOUR_STARROCKS_FE_HOSTNAME:$YOUR_STARROCKS_FE_RESFUL_PORT")
  .option("user", "$YOUR_STARROCKS_USERNAME")
  .option("password", "$YOUR_STARROCKS_PASSWORD")
  .load()

starrocksSparkDF.show(5)
```

### RDD

```scala
import com.starrocks.connector.spark._
val starrocksSparkRDD = sc.starrocksRDD(
  tableIdentifier = Some("$YOUR_STARROCKS_DATABASE_NAME.$YOUR_STARROCKS_TABLE_NAME"),
  cfg = Some(Map(
    "starrocks.fenodes" -> "$YOUR_STARROCKS_FE_HOSTNAME:$YOUR_STARROCKS_FE_RESFUL_PORT",
    "starrocks.request.auth.user" -> "$YOUR_STARROCKS_USERNAME",
    "starrocks.request.auth.password" -> "$YOUR_STARROCKS_PASSWORD"
  ))
)

starrocksSparkRDD.collect()
```

## Configuration

### General

| Key                                  | Default Value     | Comment                                                      |
| ------------------------------------ | ----------------- | ------------------------------------------------------------ |
| starrocks.fenodes                    | --                | StarRocks FE http address, support multiple addresses, separated by commas            |
| starrocks.table.identifier           | --                | StarRocks table identifier, eg, db1.tbl1                                 |
| starrocks.request.retries            | 3                 | Number of retries to send requests to StarRocks                                    |
| starrocks.request.connect.timeout.ms | 30000             | Connection timeout for sending requests to StarRocks                                |
| starrocks.request.read.timeout.ms    | 30000             | Read timeout for sending request to StarRocks                                |
| starrocks.request.query.timeout.s    | 3600              | Query the timeout time of StarRocks, the default is 1 hour, -1 means no timeout limit             |
| starrocks.request.tablet.size        | Integer.MAX_VALUE | The number of StarRocks Tablets corresponding to an RDD Partition. The smaller this value is set, the more partitions will be generated. This will increase the parallelism on the Spark side, but at the same time will cause greater pressure on StarRocks. |
| starrocks.batch.size                 | 1024              | The maximum number of rows to read data from BE at one time. Increasing this value can reduce the number of connections between Spark and StarRocks. Thereby reducing the extra time overhead caused by network delay. |
| starrocks.exec.mem.limit             | 2147483648        | Memory limit for a single query. The default is 2GB, in bytes.                     |
| starrocks.deserialize.arrow.async    | false             | Whether to support asynchronous conversion of Arrow format to RowBatch required for spark-starrocks-connector iteration                 |
| starrocks.deserialize.queue.size     | 64                | Asynchronous conversion of the internal processing queue in Arrow format takes effect when starrocks.deserialize.arrow.async is true        |

### SQL & Dataframe Configuration

| Key                                 | Default Value | Comment                                                      |
| ----------------------------------- | ------------- | ------------------------------------------------------------ |
| user                                | --            | StarRocks username                                           |
| password                            | --            | StarRocks password                                           |
| starrocks.filter.query.in.max.count | 100           | In the predicate pushdown, the maximum number of elements in the in expression value list. If this number is exceeded, the in-expression conditional filtering is processed on the Spark side. |

### RDD Configuration

| Key                             | Default Value | Comment                                                      |
| ------------------------------- | ------------- | ------------------------------------------------------------ |
| starrocks.request.auth.user     | --            | StarRocks username                                           |
| starrocks.request.auth.password | --            | StarRocks password                                           |
| starrocks.read.field            | --            | List of column names in the StarRocks table, separated by commas                  |
| starrocks.filter.query          | --            | Filter expression of the query, which is transparently transmitted to StarRocks. StarRocks uses this expression to complete source-side data filtering. |



## StarRocks & Spark Column Type Mapping

| StarRocks Type | Spark Type                       |
| -------------- | -------------------------------- |
| NULL_TYPE      | DataTypes.NullType               |
| BOOLEAN        | DataTypes.BooleanType            |
| TINYINT        | DataTypes.ByteType               |
| SMALLINT       | DataTypes.ShortType              |
| INT            | DataTypes.IntegerType            |
| BIGINT         | DataTypes.LongType               |
| FLOAT          | DataTypes.FloatType              |
| DOUBLE         | DataTypes.DoubleType             |
| DATE           | DataTypes.StringType<sup>1</sup> |
| DATETIME       | DataTypes.StringType<sup>1</sup> |
| BINARY         | DataTypes.BinaryType             |
| DECIMAL        | DecimalType                      |
| CHAR           | DataTypes.StringType             |
| LARGEINT       | DataTypes.StringType             |
| VARCHAR        | DataTypes.StringType             |
| DECIMALV2      | DecimalType                      |
| TIME           | DataTypes.DoubleType             |
| HLL            | Unsupported datatype             |

* Note: In Connector, `DATE` and` DATETIME` are mapped to `String`. Due to the processing logic of the StarRocks underlying storage engine, when the time type is used directly, the time range covered cannot meet the demand. So use `String` type to directly return the corresponding time readable text.
