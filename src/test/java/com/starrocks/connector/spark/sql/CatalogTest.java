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

import org.apache.spark.sql.SparkSession;
import org.junit.Test;

import static com.starrocks.connector.spark.sql.ITTestBase.FE_HTTP;
import static com.starrocks.connector.spark.sql.ITTestBase.FE_JDBC;
import static com.starrocks.connector.spark.sql.ITTestBase.PASSWORD;
import static com.starrocks.connector.spark.sql.ITTestBase.USER;

public class CatalogTest {

    @Test
    public void testSql() {
        SparkSession spark = SparkSession
                .builder()
                .master("local[1]")
                .appName("testSql")
                .config("spark.sql.catalog.starrocks", "com.starrocks.connector.spark.catalog.StarRocksCatalog")
                .config("spark.sql.catalog.starrocks.fe.http.url", FE_HTTP)
                .config("spark.sql.catalog.starrocks.fe.jdbc.url", FE_JDBC)
                .config("spark.sql.catalog.starrocks.password", PASSWORD)
                .config("spark.sql.catalog.starrocks.user", USER)
                .getOrCreate();

        String listDb = "show databases";
        spark.sql(listDb).show();
        String changeDb = "use starrocks.demo";
        spark.sql(changeDb).show();
        String listTables = "show tables";
        spark.sql(listTables).show();
        String selectQuery = "select * from tab1";
        spark.sql(selectQuery).show();

        String prunedColumnQuery = "select k1, v3, v4 from tab1";
        spark.sql(prunedColumnQuery).show();

        String insertQuery = "insert into tab3 select * from tab1";
        spark.sql(insertQuery).show();

        spark.stop();
    }

}
