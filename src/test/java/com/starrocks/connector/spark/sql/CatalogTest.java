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

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class CatalogTest extends ITTestBase{

    String tableName = "testSql_" + genRandomUuid();

    @Before
    public void prepare() throws Exception {
        String createStarRocksDB = String.format("CREATE DATABASE IF NOT EXISTS %s", DB_NAME);
        executeSrSQL(createStarRocksDB);
        String createStarRocksTable =
                String.format("CREATE TABLE IF NOT EXISTS `%s`.`%s` (" +
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

    @After
    public void close() throws Exception {
        String dropTable = String.format("DROP TABLE IF EXISTS `%s`.`%s` ", DB_NAME, tableName);
        executeSrSQL(dropTable);

        String dropDB = String.format("DROP Database IF EXISTS `%s` ", DB_NAME);
        executeSrSQL(dropDB);
    }

    @Test
    public void testLocalSql() throws Exception {
        SparkSession spark = SparkSession
                .builder()
                .master("local[1]")
                .appName("testLocalSql")
                .config("spark.sql.catalog.starrocks", "com.starrocks.connector.spark.catalog.StarRocksCatalog")
                .config("spark.sql.defaultCatalog", "starrocks")
                .config("spark.sql.catalog.starrocks.fe.http.url", FE_HTTP)
                .config("spark.sql.catalog.starrocks.fe.jdbc.url", FE_JDBC)
                .config("spark.sql.catalog.starrocks.password", PASSWORD)
                .config("spark.sql.catalog.starrocks.user", USER)
                .getOrCreate();

        String listDb = "show databases";
        spark.sql(listDb).show();
        String changeDb = String.format("use starrocks.%s", DB_NAME);
        spark.sql(changeDb).show();
        String listTables = "show tables";
        spark.sql(listTables).show();

        List<List<Object>> expectedData = new ArrayList<>();
        expectedData.add(Arrays.asList(1, "2", 3));
        expectedData.add(Arrays.asList(2, "3", 4));

        String insertSql = String.format("INSERT INTO %s VALUES (1, \"2\", 3), (2, \"3\", 4)", tableName);
        spark.sql(insertSql);

        List<List<Object>> actualWriteData = scanTable(DB_CONNECTION, DB_NAME, tableName);
        verifyResult(expectedData, actualWriteData);

        String selectSql = String.format("SELECT * FROM %s", tableName);
        List<Row> readRows = spark.sql(selectSql).collectAsList();
        verifyRows(expectedData, readRows);

        spark.stop();
    }

}
