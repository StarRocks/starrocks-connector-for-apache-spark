/*
 * Copyright 2021-present StarRocks, Inc. All rights reserved.
 *
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package com.starrocks.connector.spark.sql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class StructuredStreamingITTest extends ITTestBase {

    private String tableName;
    private String tableId;

    @BeforeEach
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
        executeSrSQL(createStarRocksTable);
    }

    @Test
    public void testStructuredStreaming(@TempDir() Path temporaryFolder) throws Exception {
        SparkSession spark = SparkSession
                .builder()
                .master("local[1]")
                .appName("testStructuredStreaming")
                .getOrCreate();

        StructType schema = new StructType(new StructField[] {
                new StructField("id", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("name", DataTypes.StringType, false, Metadata.empty()),
                new StructField("score", DataTypes.IntegerType, false, Metadata.empty())
        });
        Dataset<Row> df = spark.readStream()
                .option("sep", ",")
                .schema(schema)
                .format("csv")
                .load("src/test/resources/data/");

        Map<String, String> options = new HashMap<>();
        options.put("starrocks.fe.http.url", FE_HTTP);
        options.put("starrocks.fe.jdbc.url", FE_JDBC);
        options.put("starrocks.table.identifier", tableId);
        options.put("starrocks.user", USER);
        options.put("starrocks.password", PASSWORD);

        String checkpointDir = temporaryFolder.toString();
        StreamingQuery query = df.writeStream()
                .format("starrocks")
                .outputMode(OutputMode.Append())
                .options(options)
                .option("checkpointLocation", checkpointDir)
                .start();

        query.awaitTermination(10000);

        List<List<Object>> expectedData = new ArrayList<>();
        expectedData.add(Arrays.asList(3, "starrocks", 100));
        expectedData.add(Arrays.asList(4, "spark", 100));
        List<List<Object>> actualWriteData = scanTable(DB_CONNECTION, DB_NAME, tableName);
        verifyResult(expectedData, actualWriteData);

        spark.stop();
    }
}
