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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class ITTestBase {

    private static final Logger LOG = LoggerFactory.getLogger(ITTestBase.class);

    private static final boolean DEBUG_MODE = false;
    protected static String FE_HTTP = "127.0.0.1:8030";
    protected static String FE_JDBC = "jdbc:mysql://127.0.0.1:9030";
    protected static String USER = "root";
    protected static String PASSWORD = "";
    protected static String DB_NAME;
    protected static Connection DB_CONNECTION;

    @BeforeEach
    public void beforeClass() throws Exception {
            try {
                StarRocksTestEnvironment env = StarRocksTestEnvironment.getInstance();
                env.startIfNeeded();
                FE_HTTP = env.getHttpAddress();
                FE_JDBC = env.getJdbcUrl();
                USER = env.getUsername();
                PASSWORD = env.getPassword();
            } catch (Throwable t) {
                LOG.warn("Failed to start StarRocks container, ITs may be skipped if no external cluster is provided.", t);
            }

        assertTrue(FE_HTTP != null && FE_JDBC != null);
 
        DB_NAME = "sr_test_" + genRandomUuid();
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
    private static final DateTimeFormatter LOCAL_DATETIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final DateTimeFormatter LOCAL_DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    protected static void printRows(List<Row> rows) {
        if (CollectionUtils.isEmpty(rows)) {
            System.out.println("null or empty rows");
        }

        rows.forEach(row -> System.out.println(row.mkString(" | ")));
    }

    protected static Object normalize(Object value) {
        if (value instanceof scala.collection.Seq) {
            // Converts Scala WrappedArray/Seq to Java List
            return scala.collection.JavaConverters.seqAsJavaList((scala.collection.Seq<?>) value);
        }
        if (value instanceof scala.collection.Map) {
            // Converts Scala Map to Java Map
            return scala.collection.JavaConverters.mapAsJavaMap((scala.collection.Map<?, ?>) value);
        }
        return value;
    }

    protected static void verifyRows(List<List<Object>> expected, List<Row> actualRows) {
        List<List<Object>> actual = new ArrayList<>();
        for (Row row : actualRows) {
            List<Object> objects = new ArrayList<>();
            for (int i = 0; i < row.length(); i++) {
                objects.add(normalize(row.get(i)));
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
        if (object == null) {
            return "null";
        }

        String result;
        if (object instanceof scala.collection.Map) {
            scala.collection.Map<?, ?> scalaMap = (scala.collection.Map<?, ?>) object;
            Map<?, ?> javaMap = scala.collection.JavaConverters.mapAsJavaMap(scalaMap);
            StringJoiner joiner = new StringJoiner(",", "{", "}");
            javaMap.forEach((k, v) -> {
                String key = convertToStr(k, true);
                String value = convertToStr(v, true);
                joiner.add(key + ":" + value);
            });
            result = joiner.toString();
        } else if (object instanceof Map) {
            StringJoiner joiner = new StringJoiner(",", "{", "}");
            ((Map<?, ?>) object).forEach((k, v) -> {
                String key = convertToStr(k, true);
                String value = convertToStr(v, true);
                joiner.add(key + ":" + value);
            });
            result = joiner.toString();
        } else if (object instanceof Row) {
            Row row = (Row) object;
            StringJoiner joiner = new StringJoiner(",", "{", "}");
            StructType schema = row.schema();

            // Check if schema is null
            if (schema == null) {
                // Without schema, just serialize values by position
                for (int i = 0; i < row.length(); i++) {
                    Object fieldValue = row.isNullAt(i) ? null : row.get(i);
                    if (i > 0) {
                        joiner.add(convertToStr(fieldValue, true));
                    } else {
                        joiner = new StringJoiner(",", "{", "}");
                        joiner.add(convertToStr(fieldValue, true));
                    }
                }
            } else {
                // With schema, use field names
                for (int i = 0; i < row.length(); i++) {
                    String fieldName = schema.fields()[i].name();
                    Object fieldValue = row.isNullAt(i) ? null : row.get(i);
                    joiner.add("\"" + fieldName + "\":" + convertToStr(fieldValue, true));
                }
            }
            result = joiner.toString();
        } else if (object instanceof scala.collection.Seq) {
            scala.collection.Seq<?> seq = (scala.collection.Seq<?>) object;
            StringJoiner joiner = new StringJoiner(",", "[", "]");
            scala.collection.Iterator<?> iterator = seq.iterator();
            while (iterator.hasNext()) {
                Object element = iterator.next();
                String elementStr = (element == null) ? "null" : convertToStr(element, true);
                joiner.add(elementStr);
            }
            result = joiner.toString();
        } else if (object instanceof List) {
            StringJoiner joiner = new StringJoiner(",", "[", "]");
            ((List<?>) object).forEach(obj -> joiner.add(convertToStr(obj, true)));
            result = joiner.toString();
        } else if (object instanceof Timestamp) {
            result = DATETIME_FORMATTER.format((Timestamp) object);
            if (quoted) {
                return String.format("\"%s\"", result);
            }
            return result;
        } else if (object instanceof LocalDateTime) {
            result = LOCAL_DATETIME_FORMATTER.format((LocalDateTime) object);
            if (quoted) {
                return String.format("\"%s\"", result);
            }
            return result;
        } else if (object instanceof LocalDate || object instanceof Date) {
            if (object instanceof LocalDate) {
                result = LOCAL_DATE_FORMATTER.format((LocalDate) object);
            } else {
                result = object.toString();
            }
            if (quoted) {
                return String.format("\"%s\"", result);
            }
            return result;
        } else {
            result = object.toString();
        }

        if (quoted && object instanceof String) {
            return String.format("\"%s\"", result);
        } else {
            return result;
        }
    }
}
