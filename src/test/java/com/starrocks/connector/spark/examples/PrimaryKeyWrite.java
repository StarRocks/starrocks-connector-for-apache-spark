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

package com.starrocks.connector.spark.examples;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

public class PrimaryKeyWrite {

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
        partialUpdate();
    }

    // Show how to do partial update for columns id and name
    private static void partialUpdate() {
        // 1. create a spark session
        SparkSession spark = SparkSession
                .builder()
                .config(new SparkConf())
                .master("local[1]")
                .appName("sqlWrite")
                .getOrCreate();

        // 2. create a datasource using "starrocks" identifier
        // use starrocks.columns to specify the columns to update
        // use starrocks.write.properties.partial_update to enable partial update
        spark.sql("CREATE TABLE starrocks_table \n" +
                " USING starrocks\n" +
                "OPTIONS(\n" +
                "  \"starrocks.fe.http.url\"=\"127.0.0.1:11901\",\n" +
                "  \"starrocks.fe.jdbc.url\"=\"jdbc:mysql://127.0.0.1:11903\",\n" +
                "  \"starrocks.table.identifier\"=\"" + TABLE_ID + "\",\n" +
                "  \"starrocks.user\"=\"root\",\n" +
                "  \"starrocks.password\"=\"\",\n" +
                "  \"starrocks.columns\"=\"id,name\",\n" +
                "  \"starrocks.write.properties.partial_update\"=\"true\"\n" +
                ");");

        // 3. insert two rows into the table
        spark.sql("INSERT INTO starrocks_table VALUES (5, \"row7\"), (6, \"row8\")");

        // 4. select the data
        spark.sql("SELECT * FROM starrocks_table").show();

        spark.stop();
    }
}
