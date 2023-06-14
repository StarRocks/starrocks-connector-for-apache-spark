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

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.Ignore;
import org.junit.Test;

@Ignore
public class ReadITTest {

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
    private static final String TABLE_ID = "starrocks.read_it_test";
    private static final String USER = "root";
    private static final String PASSWORD = "";

    @Test
    public void testDataFrame() throws Exception {
        SparkSession spark = SparkSession
                .builder()
                .master("local[1]")
                .appName("testDataFrame")
                .getOrCreate();

        Dataset<Row> df = spark.read().format("starrocks")
                .option("starrocks.table.identifier", TABLE_ID)
                .option("starrocks.fenodes", FE_HTTP)
                .option("user", USER)
                .option("password", PASSWORD)
                .load();

        df.show(5);
        spark.stop();
    }

    @Test
    public void testSql() throws Exception {
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
                "  \"user\"=\"%s\",\n" +
                "  \"password\"=\"%s\"\n" +
                ")", TABLE_ID, FE_HTTP, USER, PASSWORD);
        spark.sql(ddl);
        spark.sql("SELECT * FROM src").show(5);
        spark.stop();
    }

    @Test
    public void testStarRocksUserAndPassword() {
        SparkSession spark = SparkSession
                .builder()
                .master("local[1]")
                .appName("testDataFrame")
                .getOrCreate();

        Dataset<Row> df = spark.read().format("starrocks")
                .option("starrocks.table.identifier", TABLE_ID)
                .option("starrocks.fenodes", FE_HTTP)
                .option("starrocks.user", USER)
                .option("starrocks.password", PASSWORD)
                .load();

        df.show(5);
        spark.stop();
    }
}
