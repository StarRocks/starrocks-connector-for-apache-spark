// Copyright 2021-present StarRocks, Inc. All rights reserved.
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

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ConfigurationITTest extends ITTestBase {
    
    private String tableName;
    private String tableId;

    @Before
    public void prepare() throws Exception {
        this.tableName = "testConfig_" + genRandomUuid();
        this.tableId = String.join(".", DB_NAME, tableName);
        String createStarRocksTable =
                String.format("CREATE TABLE `%s`.`%s` (" +
                                "id INT," +
                                "name STRING," +
                                "score INT" +
                                ") ENGINE=OLAP " +
                                "PRIMARY KEY(`id`) " +
                                "DISTRIBUTED BY HASH(`id`) BUCKETS 2 " +
                                "PROPERTIES (" +
                                "\"replication_num\" = \"1\"" +
                                ")",
                        DB_NAME, tableName);
        executeSRDDLSQL(createStarRocksTable);
    }

    @Test
    public void testReadOldConfig() throws Exception {
        SparkSession spark = SparkSession
                .builder()
                .config(new SparkConf())
                .master("local[1]")
                .appName("testSql")
                .getOrCreate();

        String ddl = String.format("CREATE TABLE src \n" +
                " USING starrocks\n" +
                "OPTIONS(\n" +
                "  \"starrocks.table.identifier\"=\"%s\",\n" +
                "  \"starrocks.fenodes\"=\"%s\",\n" +
                "  \"starrocks.fe.jdbc.url\"=\"%s\",\n" +
                "  \"user\"=\"%s\",\n" +
                "  \"password\"=\"%s\"\n" +
                ")", tableId, FE_HTTP, FE_JDBC, USER, PASSWORD);
        spark.sql(ddl).collect();

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
                new StructField("score", DataTypes.IntegerType, false, Metadata.empty())
        });

        Dataset<Row> df = spark.createDataFrame(data, schema);

        Map<String, String> options = new HashMap<>();
        options.put("starrocks.fe.http.url", FE_HTTP);
        options.put("starrocks.fe.jdbc.url", FE_JDBC);
        options.put("starrocks.table.identifier", tableId);
        options.put("starrocks.user", USER);
        options.put("starrocks.password", PASSWORD);

        df.write().format("starrocks")
                .mode(SaveMode.Append)
                .options(options)
                .save();

        spark.stop();
    }

    @Test
    public void testWriteCsvConfiguration() throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put("starrocks.write.properties.format", "csv");
        options.put("starrocks.write.properties.row_delimiter", "|");
        options.put("starrocks.write.properties.column_separator", ",");
        testWriteConfigurationBase(options);
    }

    @Test
    public void testWriteJsonConfiguration() throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put("starrocks.write.properties.format", "json");
        testWriteConfigurationBase(options);
    }

    @Test
    public void testTransactionConfiguration() throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put("starrocks.write.enable.transaction-stream-load", "false");
        testWriteConfigurationBase(options);
    }

    private void testWriteConfigurationBase(Map<String, String> customOptions) throws Exception {
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
                new StructField("score", DataTypes.IntegerType, false, Metadata.empty())
        });

        Dataset<Row> df = spark.createDataFrame(data, schema);

        Map<String, String> options = new HashMap<>();
        options.put("starrocks.fe.http.url", FE_HTTP);
        options.put("starrocks.fe.jdbc.url", FE_JDBC);
        options.put("starrocks.table.identifier", tableId);
        options.put("starrocks.user", USER);
        options.put("starrocks.password", PASSWORD);
        options.put("starrocks.request.retries", "4");
        options.put("starrocks.request.connect.timeout.ms", "40000");
        options.put("starrocks.request.read.timeout.ms", "5000");
        options.put("starrocks.columns", "id,name,score");
        options.put("starrocks.write.label.prefix", "spark-connector-");
        options.put("starrocks.write.wait-for-continue.timeout.ms", "10000");
        options.put("starrocks.write.chunk.limit", "100k");
        options.put("starrocks.write.scan-frequency.ms", "100");
        options.put("starrocks.write.enable.transaction-stream-load", "true");
        options.put("starrocks.write.buffer.size", "12k");
        options.put("starrocks.write.flush.interval.ms", "3000");
        options.put("starrocks.write.max.retries", "2");
        options.put("starrocks.write.retry.interval.ms", "1000");
        options.put("starrocks.write.properties.format", "csv");
        options.put("starrocks.write.properties.row_delimiter", "\n");
        options.put("starrocks.write.properties.column_separator", "\t");
        options.putAll(customOptions);

        df.write().format("starrocks")
                .mode(SaveMode.Append)
                .options(options)
                .save();

        spark.stop();
    }
}
