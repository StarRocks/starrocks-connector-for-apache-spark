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

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.FixMethodOrder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.runners.MethodSorters;

import java.util.List;
import java.util.Objects;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@FixMethodOrder(MethodSorters.JVM)
public class CatalogTest extends BypassModeTestBase {

    @BeforeEach
    public void beforeEach() throws Exception {
        ITTestBase.beforeClass();
        clean();
    }

    @AfterEach
    public void afterEach() throws Exception {
        clean();
        ITTestBase.afterClass();
    }

    @Test
    public void testCatalog() throws Throwable {
        withSparkSession(spark -> {
            {
                Dataset<Row> df = spark.sql("SHOW DATABASES");
                df.show();
                List<Row> rows = df.collectAsList();
                assertTrue(rows.stream().noneMatch(r -> DB_NAME.equals(r.getAs("namespace"))));
            }

            {
                executeSrSQL(String.format("CREATE DATABASE IF NOT EXISTS %s", DB_NAME));
                Dataset<Row> df = spark.sql("SHOW DATABASES");
                df.show();
                List<Row> rows = df.collectAsList();
                assertTrue(rows.stream().anyMatch(r -> DB_NAME.equals(r.getAs("namespace"))));
            }

            spark.sql(String.format("USE starrocks.%s", DB_NAME));

            {
                Dataset<Row> df = spark.sql("SHOW TABLES");
                df.show();
                List<Row> rows = df.collectAsList();
                assertEquals(0, rows.size());
            }

            for (String table : TABLES) {
                executeSrSQL(loadSql(table));
            }

            {
                Dataset<Row> df = spark.sql("SHOW TABLES");
                df.show();
                List<Row> rows = df.collectAsList();
                assertArrayEquals(
                        TABLES.stream().sorted(String::compareTo).toArray(String[]::new),
                        rows.stream()
                                .map(row -> Objects.toString(row.getAs("tableName")))
                                .sorted(String::compareTo).toArray(String[]::new));
            }
        });
    }

}
