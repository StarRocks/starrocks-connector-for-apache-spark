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

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
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
import java.util.Properties;
import java.util.StringJoiner;
import java.util.UUID;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.io.IOUtils;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class ITTestBase {

    private static final Logger LOG = LoggerFactory.getLogger(ITTestBase.class);

    protected static String FE_HTTP = "10.37.42.50:8031";
    protected static String FE_JDBC = "jdbc:mysql://10.37.42.50:9031";
    protected static String USER = "root";
    protected static String PASSWORD = "";
    private static final boolean DEBUG_MODE = false;
    protected static final String DB_NAME = "sr_spark_test_db";

    protected static Connection DB_CONNECTION;

    @BeforeEach
    public void beforeClass() throws Exception {
        assumeTrue(DEBUG_MODE);
        Properties props = loadConnProps();
        FE_HTTP = props.getProperty("starrocks.fe.http.url", FE_HTTP);
        FE_JDBC = props.getProperty("starrocks.fe.jdbc.url", FE_JDBC);
        USER = props.getProperty("starrocks.user", USER);
        PASSWORD = props.getProperty("starrocks.password", PASSWORD);

        try {
            DB_CONNECTION = DriverManager.getConnection(FE_JDBC, USER, PASSWORD);
            LOG.info("Success to create db connection via jdbc {}", FE_JDBC);
        } catch (Exception e) {
            LOG.error("Failed to create db connection via jdbc {}", FE_JDBC, e);
            throw e;
        }

        try {
            executeSrSQL(String.format("CREATE DATABASE IF NOT EXISTS %s", DB_NAME));
            LOG.info("Successful to create database {}", DB_NAME);
        } catch (Exception e) {
            LOG.error("Failed to create database {}", DB_NAME, e);
            throw e;
        }
    }

    protected static Properties loadConnProps() throws IOException {
        try (InputStream inputStream = ITTestBase.class.getClassLoader()
                .getResourceAsStream("starrocks_conn.properties")) {
            Properties props = new Properties();
            props.load(inputStream);
            return props;
        }
    }

    protected static String loadSqlTemplate(String filepath) throws IOException {
        try (InputStream inputStream = ITTestBase.class.getClassLoader().getResourceAsStream(filepath)) {
            return IOUtils.toString(
                    requireNonNull(inputStream, "null input stream when load '" + filepath + "'"),
                    StandardCharsets.UTF_8);
        }
    }

    @AfterEach
    public void afterClass() throws Exception {
        if (DB_CONNECTION != null) {
            try {
                executeSrSQL(String.format("DROP DATABASE IF EXISTS %s FORCE", DB_NAME));
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

    protected static void printRows(List<Row> rows) {
        if (CollectionUtils.isEmpty(rows)) {
            System.out.println("null or empty rows");
        }

        rows.forEach(row -> System.out.println(row.mkString(" | ")));
    }

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
        } else if (object instanceof Timestamp) {
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
