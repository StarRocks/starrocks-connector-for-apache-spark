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

package com.starrocks.connector.spark.examples;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A simple example to write data to a starrocks primary key table using DataFrame/SQL.
 */
public class SimpleWrite {

    // StarRocks table DDL
    //
    // CREATE TABLE `score_board` (
    //     `id` int(11) NOT NULL COMMENT "",
    //     `name` varchar(65533) NULL DEFAULT "" COMMENT "",
    //     `score` int(11) NOT NULL DEFAULT "0" COMMENT ""
    // ) ENGINE=OLAP
    // PRIMARY KEY(`id`)
    // COMMENT "OLAP"
    // DISTRIBUTED BY HASH(`id`)
    // PROPERTIES (
    //      "replication_num" = "1"
    // );

    private static final String FE_HTTP = "127.0.0.1:11901";
    private static final String FE_JDBC = "jdbc:mysql://127.0.0.1:11903";
    private static final String DB = "test";
    private static final String TABLE = "score_board";
    private static final String TABLE_ID = DB + "." + TABLE;
    private static final String USER = "root";
    private static final String PASSWORD = "";

    public static void main(String[] args) throws Exception {
        dataFrameBatchWrite();
        dataFrameSteamingWrite();
        sqlWrite();
        sqlWriteThroughCatalog();
    }

    // write using DataFrame in batch mode
    private static void dataFrameBatchWrite() {
        // 1. create a spark session
        SparkSession spark = SparkSession
                .builder()
                .master("local[1]")
                .appName("dataFrameBatchWrite")
                .getOrCreate();

        // 2. create a source DataFrame from a list of data, and define
        // the schema which is mapped to the StarRocks table
        List<Row> data = Arrays.asList(
                RowFactory.create(1, "row1", 1),
                RowFactory.create(2, "row2", 2)
        );
        StructType schema = new StructType(new StructField[] {
                new StructField("id", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("name", DataTypes.StringType, true, Metadata.empty()),
                new StructField("score", DataTypes.IntegerType, true, Metadata.empty())
        });
        Dataset<Row> df = spark.createDataFrame(data, schema);

        // 3. create starrocks writer with the necessary options.
        // The format for the writer is "starrocks"

        Map<String, String> options = new HashMap<>();
        // FE http url like "127.0.0.1:11901"
        options.put("starrocks.fe.http.url", FE_HTTP);
        // FE jdbc url like "jdbc:mysql://127.0.0.1:11903"
        options.put("starrocks.fe.jdbc.url", FE_JDBC);
        // table identifier
        options.put("starrocks.table.identifier", TABLE_ID);
        // starrocks username
        options.put("starrocks.user", USER);
        // starrocks password
        options.put("starrocks.password", PASSWORD);

        // The format should be "starrocks"
        df.write().format("starrocks")
                .mode(SaveMode.Append)
                .options(options)
                .save();

        spark.stop();
    }

    // write using DataFrame in structured streaming mode
    private static void dataFrameSteamingWrite() throws Exception {
        // 1. create a spark session
        SparkSession spark = SparkSession
                .builder()
                .master("local[1]")
                .appName("dataFrameSteamingWrite")
                .getOrCreate();

        // 2. create a streaming source DataFrame from csv files
        StructType schema = new StructType(new StructField[] {
                new StructField("id", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("name", DataTypes.StringType, false, Metadata.empty()),
                new StructField("score", DataTypes.IntegerType, false, Metadata.empty())
        });
        Dataset<Row> df = spark.readStream()
                .option("sep", ",")
                .schema(schema)
                .format("csv")
                .load("src/test/resources/simple_write_csv/");

        // 3. create starrocks writer with the necessary options.
        // The format for the writer is "starrocks"

        Map<String, String> options = new HashMap<>();
        // FE http url like "127.0.0.1:11901"
        options.put("starrocks.fe.http.url", FE_HTTP);
        // FE jdbc url like "jdbc:mysql://127.0.0.1:11903"
        options.put("starrocks.fe.jdbc.url", FE_JDBC);
        // table identifier
        options.put("starrocks.table.identifier", TABLE_ID);
        // starrocks username
        options.put("starrocks.user", USER);
        // starrocks password
        options.put("starrocks.password", PASSWORD);

        StreamingQuery query = df.writeStream()
                // The format should be "starrocks"
                .format("starrocks")
                .outputMode(OutputMode.Append())
                .options(options)
                // set your checkpoint location
                .option("checkpointLocation", "checkpoint")
                .start();

        query.awaitTermination();

        spark.stop();
    }

    private static void sqlWrite() {
        // 1. create a spark session
        SparkSession spark = SparkSession
                .builder()
                .config(new SparkConf())
                .master("local[1]")
                .appName("sqlWrite")
                .getOrCreate();

        // 2. create a datasource using "starrocks" identifier
        spark.sql("CREATE TABLE starrocks_table \n" +
                " USING starrocks\n" +
                "OPTIONS(\n" +
                "  \"starrocks.fe.http.url\"=\"127.0.0.1:11901\",\n" +
                "  \"starrocks.fe.jdbc.url\"=\"jdbc:mysql://127.0.0.1:11903\",\n" +
                "  \"starrocks.table.identifier\"=\"" + TABLE_ID + "\",\n" +
                "  \"starrocks.user\"=\"root\",\n" +
                "  \"starrocks.password\"=\"\"\n" +
                ");");

        // 3. insert two rows into the table
        spark.sql("INSERT INTO starrocks_table VALUES (5, \"row5\", 5), (6, \"row6\", 6)");

        // 4. select the data
        spark.sql("SELECT * FROM starrocks_table").show();

        spark.stop();
    }

    private static void sqlWriteThroughCatalog() {
        // 1. create a spark session
        SparkSession spark = SparkSession
                .builder()
                .config(new SparkConf()
                        .set("spark.sql.catalog.sr", "com.starrocks.connector.spark.catalog.StarRocksCatalog")
                        .set("spark.sql.catalog.sr.starrocks.fe.http.url", FE_HTTP)
                        .set("spark.sql.catalog.sr.starrocks.fe.jdbc.url", FE_JDBC)
                        .set("spark.sql.catalog.sr.starrocks.user", "root")
                        .set("spark.sql.catalog.sr.starrocks.password", "")
                        )
                .master("local[1]")
                .appName("sqlWriteThroughCatalog")
                .getOrCreate();

        // 2. insert two rows into the table
        spark.sql("INSERT INTO sr.starrocks_table VALUES (5, \"row5\", 5), (6, \"row6\", 6)");

        // 3. select the data
        spark.sql("SELECT * FROM sr.starrocks_table").show();

        spark.stop();
    }
}
