// Modifications Copyright 2021 StarRocks Limited.
//
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.connector.spark.sql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.streaming.MemoryStream;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.Ignore;
import org.junit.Test;
import scala.Option;
import scala.collection.JavaConverters;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Ignore
public class WriteITTest {

// StarRocks table
//    CREATE TABLE `read_it_test` (
//        `id` int(11) NULL COMMENT "",
//        `name` varchar(65533) NULL COMMENT "",
//        `age` int(11) NULL COMMENT ""
//    ) ENGINE=OLAP
//    DUPLICATE KEY(`id`)
//    COMMENT "OLAP"
//    DISTRIBUTED BY HASH(`id`) BUCKETS 2
//    PROPERTIES (
//    "replication_num" = "1",
//        "in_memory" = "false",
//        "storage_format" = "DEFAULT",
//        "enable_persistent_index" = "false",
//        "replicated_storage" = "true",
//        "compression" = "LZ4"
//    );


    private static final String FE_HTTP = "127.0.0.1:11901";
    private static final String FE_JDBC = "jdbc:mysql://127.0.0.1:11903";
    private static final String DB = "starrocks";
    private static final String TABLE = "read_it_test";
    private static final String TABLE_ID = DB + "." + TABLE;
    private static final String USER = "root";
    private static final String PASSWORD = "";

    @Test
    public void testDataFrame() {
        SparkSession spark = SparkSession
                .builder()
                .master("local[1]")
                .appName("testDataFrame")
                .getOrCreate();

        List<Row> data = Arrays.asList(
                RowFactory.create(1, "2", 3),
                RowFactory.create(2, "3", 4)
        );

        StructType schema = new StructType(new StructField[]{
                new StructField("id", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("name", DataTypes.StringType, false, Metadata.empty()),
                new StructField("age", DataTypes.IntegerType, false, Metadata.empty())
        });

        Dataset<Row> df = spark.createDataFrame(data, schema);

        Map<String, String> options = new HashMap<>();
        options.put("starrocks.fenodes", FE_HTTP);
        options.put("starrocks.fe.jdbc.url", FE_JDBC);
        options.put("starrocks.table.identifier", TABLE_ID);
        options.put("starrocks.user", USER);
        options.put("starrocks.password", PASSWORD);

        df.write().format("starrocks_writer")
                .mode(SaveMode.Append)
                .options(options)
                .save();

        spark.stop();
    }

    @Test
    public void testDefaultConfiguration() throws Exception {
        SparkSession spark = SparkSession
                .builder()
                .master("local[1]")
                .appName("testDataFrame")
                .getOrCreate();

        List<Row> data = Arrays.asList(
                RowFactory.create(1, "2", 3),
                RowFactory.create(2, "3", 4)
        );

        StructType schema = new StructType(new StructField[]{
                new StructField("id", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("name", DataTypes.StringType, false, Metadata.empty()),
                new StructField("age", DataTypes.IntegerType, false, Metadata.empty())
        });

        Dataset<Row> df = spark.createDataFrame(data, schema);

        Map<String, String> options = new HashMap<>();
        options.put("starrocks.fenodes", FE_HTTP);
        options.put("starrocks.fe.jdbc.url", FE_JDBC);
        options.put("starrocks.table.identifier", TABLE_ID);
        options.put("starrocks.user", USER);
        options.put("starrocks.password", PASSWORD);

        df.write().format("starrocks_writer")
                .mode(SaveMode.Append)
                .options(options)
                .save();

        spark.stop();
    }

    @Test
    public void testCsvConfiguration() throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put("starrocks.write.properties.format", "csv");
        options.put("starrocks.write.properties.row_delimiter", "|");
        options.put("starrocks.write.properties.column_separator", ",");
        testConfigurationBase(options);
    }

    @Test
    public void testJsonConfiguration() throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put("starrocks.write.properties.format", "json");
        testConfigurationBase(options);
    }

    @Test
    public void testTransactionConfiguration() throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put("starrocks.write.enable.transaction-stream-load", "false");
        testConfigurationBase(options);
    }

    private void testConfigurationBase(Map<String, String> customOptions) throws Exception {
        SparkSession spark = SparkSession
                .builder()
                .master("local[1]")
                .appName("testDataFrame")
                .getOrCreate();

        List<Row> data = Arrays.asList(
                RowFactory.create(1, "2", 3),
                RowFactory.create(2, "3", 4)
        );

        StructType schema = new StructType(new StructField[]{
                new StructField("id", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("name", DataTypes.StringType, false, Metadata.empty()),
                new StructField("age", DataTypes.IntegerType, false, Metadata.empty())
        });

        Dataset<Row> df = spark.createDataFrame(data, schema);

        Map<String, String> options = new HashMap<>();
        options.put("starrocks.fenodes", FE_HTTP);
        options.put("starrocks.fe.jdbc.url", FE_JDBC);
        options.put("starrocks.table.identifier", TABLE_ID);
        options.put("starrocks.user", USER);
        options.put("starrocks.password", PASSWORD);
        options.put("starrocks.request.retries", "4");
        options.put("starrocks.request.connect.timeout.ms", "40000");
        options.put("starrocks.request.read.timeout.ms", "5000");
        options.put("starrocks.columns", "id,name,age");
        options.put("starrocks.write.label.prefix", "spark-connector-");
        options.put("starrocks.write.wait-for-continue.timeout.ms", "10000");
        options.put("starrocks.write.chunk.limit", "102400");
        options.put("starrocks.write.scan-frequency.ms", "100");
        options.put("starrocks.write.enable.transaction-stream-load", "true");
        options.put("starrocks.write.buffer.size", "12000");
        options.put("starrocks.write.flush.interval.ms", "3000");
        options.put("starrocks.write.properties.format", "csv");
        options.put("starrocks.write.properties.row_delimiter", "\n");
        options.put("starrocks.write.properties.column_separator", "\t");
        options.putAll(customOptions);

        df.write().format("starrocks_writer")
                .mode(SaveMode.Append)
                .options(options)
                .save();

        spark.stop();
    }

    @Ignore
    @Test
    public void testStructuredStreaming() throws Exception {
        SparkSession spark = SparkSession
                .builder()
                .master("local[1]")
                .appName("testStructuredStreaming")
                .getOrCreate();

        List<Row> data = Arrays.asList(
                RowFactory.create(1, "70"),
                RowFactory.create(2, "80")
        );
        StructType schema = new StructType(new StructField[]{
                new StructField("c0", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("c1", DataTypes.StringType, false, Metadata.empty())
        });
        spark.createDataFrame(data, schema);

        MemoryStream<Row> memoryStream = new MemoryStream<>(0, spark.sqlContext(), Option.empty(), Encoders.javaSerialization(Row.class));
        Dataset<Row> df = memoryStream.toDF();
        memoryStream.addData(JavaConverters.asScalaIteratorConverter(data.iterator()).asScala().toSeq());

        Map<String, String> options = new HashMap<>();
        options.put("spark.starrocks.fe.urls.http", "127.0.0.1:11901");
        options.put("spark.starrocks.fe.urls.jdbc", "jdbc:mysql://127.0.0.1:11903");
        options.put("spark.starrocks.database", "starrocks");
        options.put("spark.starrocks.table", "test");
        options.put("spark.starrocks.username", "root");
        options.put("spark.starrocks.password", "");
        options.put("checkpointLocation", "/Users/lpf/Downloads/spark-cpt");

        StreamingQuery query = df.writeStream().format("starrocks_writer")
                .options(options)
                .start();

        query.awaitTermination();
        spark.stop();
    }
}
