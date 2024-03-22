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
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.Seq;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.StringJoiner;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assume.assumeTrue;

public abstract class ITTestBase {

    private static final Logger LOG = LoggerFactory.getLogger(ITTestBase.class);

    protected static String FE_HTTP;
    protected static String FE_JDBC;
    protected static String USER = "root";
    protected static String PASSWORD = "";
    private static final boolean DEBUG_MODE = false;
    protected static String DB_NAME;

    protected static Connection DB_CONNECTION;

    @BeforeClass
    public static void setUp() throws Exception {
        FE_HTTP = DEBUG_MODE ? "127.0.0.1:8030" : System.getProperty("http_urls");
        FE_JDBC = DEBUG_MODE ? "jdbc:mysql://127.0.0.1:9030" : System.getProperty("jdbc_urls");
        assumeTrue(FE_HTTP != null && FE_JDBC != null);

        DB_NAME = "sr_spark_test_" + genRandomUuid();
        try {
            DB_CONNECTION = DriverManager.getConnection(FE_JDBC, "root", "");
            LOG.info("Success to create db connection via jdbc {}", FE_JDBC);
        } catch (Exception e) {
            LOG.error("Failed to create db connection via jdbc {}", FE_JDBC, e);
            throw e;
        }

        try {
            String createDb = "CREATE DATABASE " + DB_NAME;
            executeSrSQL(createDb);
            LOG.info("Successful to create database {}", DB_NAME);
        } catch (Exception e) {
            LOG.error("Failed to create database {}", DB_NAME, e);
            throw e;
        }
    }

    @AfterClass
    public static void tearDown() throws Exception {
        if (DB_CONNECTION != null) {
            try {
                String dropDb = String.format("DROP DATABASE IF EXISTS %s FORCE", DB_NAME);
                executeSrSQL(dropDb);
                LOG.info("Successful to drop database {}", DB_NAME);
            } catch (Exception e) {
                LOG.error("Failed to drop database {}", DB_NAME, e);
            }
            DB_CONNECTION.close();
        }
    }

    protected static String genRandomUuid() {
        return UUID.randomUUID().toString().replace("-", "_");
    }

    protected static void executeSrSQL(String sql) throws Exception {
        try (PreparedStatement statement = DB_CONNECTION.prepareStatement(sql)) {
            statement.execute();
        }
    }

    protected static String addHttpSchemaPrefix(String feHosts, String prefix) {
        String[] hosts = feHosts.split(",");
        return Arrays.stream(hosts).map(h -> prefix + h).collect(Collectors.joining(","));
    }

    protected static List<List<Object>> scanTable(Connection dbConnector, String db, String table) throws SQLException {
        String query = String.format("SELECT * FROM `%s`.`%s`", db, table);
        return queryTable(dbConnector, query);
    }

    protected static List<List<Object>> queryTable(Connection dbConnector, String query) throws SQLException {
        try (PreparedStatement statement = dbConnector.prepareStatement(query)) {
            try (ResultSet resultSet = statement.executeQuery()) {
                List<List<Object>> results = new ArrayList<>();
                int numColumns = resultSet.getMetaData().getColumnCount();
                while (resultSet.next()) {
                    List<Object> row = new ArrayList<>();
                    for (int i = 1; i <= numColumns; i++) {
                        row.add(resultSet.getObject(i));
                    }
                    results.add(row);
                }
                return results;
            }
        }
    }

    private static final SimpleDateFormat DATETIME_FORMATTER = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    protected static void verifyRows(List<List<Object>> expected, List<Row> actualRows) {
        List<List<Object>> actual = new ArrayList<>();
        for (Row row : actualRows) {
            List<Object> objects = new ArrayList<>();
            for (int i = 0; i < row.length(); i++) {
                objects.add(row.get(i));
            }
            actual.add(objects);
        }
        verifyResult(expected, actual);
    }

    protected static void verifyResult(List<List<Object>> expected, List<List<Object>> actual) {
        List<String> expectedRows = new ArrayList<>();
        List<String> actualRows = new ArrayList<>();
        for (List<Object> row : expected) {
            StringJoiner joiner = new StringJoiner(",");
            for (Object col : row) {
                joiner.add(convertToStr(col, false));
            }
            expectedRows.add(joiner.toString());
        }
        expectedRows.sort(String::compareTo);

        for (List<Object> row : actual) {
            StringJoiner joiner = new StringJoiner(",");
            for (Object col : row) {
                joiner.add(convertToStr(col, false));
            }
            actualRows.add(joiner.toString());
        }
        actualRows.sort(String::compareTo);
        assertArrayEquals(expectedRows.toArray(), actualRows.toArray());
    }

    private static String convertToStr(Object object, boolean quoted) {
        String result;
        if (object instanceof List) {
            // for array type
            StringJoiner joiner = new StringJoiner(",", "[", "]");
            ((List<?>) object).forEach(obj -> joiner.add(convertToStr(obj, true)));
            result = joiner.toString();
        } else if (object instanceof  Seq) {
            // for scala array type
            StringJoiner joiner = new StringJoiner(",", "[", "]");
            ((Seq<?>) object).foreach(obj -> joiner.add(convertToStr(obj, true)));
            result = joiner.toString();
        }else if (object instanceof Timestamp) {
            result = DATETIME_FORMATTER.format((Timestamp) object);
        } else {
            result = object == null ? "null" : object.toString();
        }

        if (quoted && (object instanceof String || object instanceof Date)) {
            return String.format("\"%s\"", result);
        } else {
            return result;
        }
    }
}
