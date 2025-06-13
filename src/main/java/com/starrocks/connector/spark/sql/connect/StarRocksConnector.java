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

package com.starrocks.connector.spark.sql.connect;

import com.starrocks.connector.spark.exception.StarRocksException;
import com.starrocks.connector.spark.sql.conf.StarRocksConfig;
import com.starrocks.connector.spark.sql.schema.StarRocksField;
import com.starrocks.connector.spark.sql.schema.StarRocksSchema;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class StarRocksConnector {
    private static Logger logger = LoggerFactory.getLogger(StarRocksConnector.class);

    private static final String TABLE_SCHEMA_QUERY =
            "SELECT `COLUMN_NAME`, `ORDINAL_POSITION`, `COLUMN_KEY`, `DATA_TYPE`, `COLUMN_SIZE`, `DECIMAL_DIGITS` "
                    + "FROM `information_schema`.`COLUMNS` WHERE `TABLE_SCHEMA`=? AND `TABLE_NAME`=?;";
    private static final String ALL_DBS_QUERY = "show databases;";
    private static final String LOAD_DB_QUERY =
            "select SCHEMA_NAME from information_schema.schemata where SCHEMA_NAME in (?) AND CATALOG_NAME = 'def';";
    private static final String ALL_TABLES_QUERY = "select TABLE_SCHEMA, TABLE_NAME from information_schema.tables "
            + "where TABLE_TYPE = 'BASE TABLE' AND TABLE_SCHEMA in (?) ;";

    // Driver name for mysql connector 5.1 which is deprecated in 8.0
    private static final String MYSQL_51_DRIVER_NAME = "com.mysql.jdbc.Driver";
    // Driver name for mysql connector 8.0
    private static final String MYSQL_80_DRIVER_NAME = "com.mysql.cj.jdbc.Driver";
    private static final String MYSQL_SITE_URL = "https://dev.mysql.com/downloads/connector/j/";
    private static final String MAVEN_CENTRAL_URL = "https://repo1.maven.org/maven2/mysql/mysql-connector-java/";

    public static StarRocksSchema getSchema(StarRocksConfig config, Identifier tbIdentifier) {
        String database = tbIdentifier == null ? config.getDatabase() : Arrays.stream(tbIdentifier.namespace()).collect(
                Collectors.joining("."));
        String table =  tbIdentifier == null ? config.getTable() : tbIdentifier.name();
        List<String> parameters = Arrays.asList(database, table);
        List<Map<String, String>> columnValues = extractColumnValuesBySql(config, TABLE_SCHEMA_QUERY, parameters);
        List<StarRocksField> pks = new ArrayList<>();
        List<StarRocksField> columns = new ArrayList<>();
        for (Map<String, String> columnValue : columnValues) {
            StarRocksField field = new StarRocksField(columnValue.get("COLUMN_NAME"), columnValue.get("DATA_TYPE"),
                    Integer.parseInt(columnValue.get("ORDINAL_POSITION")),
                    Optional.ofNullable(columnValue.get("COLUMN_SIZE")).map(Integer::parseInt).orElse(null),
                    Optional.ofNullable(columnValue.get("COLUMN_SIZE")).map(Integer::parseInt).orElse(null),
                    Optional.ofNullable(columnValue.get("DECIMAL_DIGITS")).map(Integer::parseInt).orElse(null));
            columns.add(field);
            if ("PRI".equals(columnValue.get("COLUMN_KEY"))) {
                pks.add(field);
            }
        }
        columns.sort(Comparator.comparingInt(StarRocksField::getOrdinalPosition));

        return new StarRocksSchema(columns, pks);
    }

    public static List<String> getDatabases(StarRocksConfig config) {
        List<Map<String, String>> dbs = extractColumnValuesBySql(config, ALL_DBS_QUERY, Arrays.asList());
        List<String> dbNames = new ArrayList<>();

        for (Map<String, String> db : dbs) {
            String dbName = Optional.ofNullable(db.get("Database"))
                    .orElseThrow(() -> new StarRocksException("get Database header error"));
            dbNames.add(dbName);
        }

        return dbNames;
    }

    public static Map<String, String> loadDatabase(StarRocksConfig config, List<String> namespace) {
        String fullName = StringUtils.join(namespace, ".");
        if (namespace.size() != 1) {
            throw new StarRocksException("namespace should only 1, " + fullName);
        }
        List<Map<String, String>> dbs =
                extractColumnValuesBySql(config, LOAD_DB_QUERY, Arrays.asList(namespace.get(namespace.size() - 1)));

        for (Map<String, String> db : dbs) {
            String dbName = Optional.ofNullable(db.get("SCHEMA_NAME"))
                    .orElseThrow(() -> new StarRocksException("get Database SCHEMA_NAME error"));
            return new DatabaseSpec(dbName).toJavaMap();
        }

        throw new StarRocksException("database(s) not found: " + fullName);
    }

    public static Map<String, String> getTables(StarRocksConfig config, List<String> dbNames) {
        List<String> parameters = Arrays.asList(String.join(",", dbNames));
        List<Map<String, String>> tables = extractColumnValuesBySql(config, ALL_TABLES_QUERY, parameters);
        Map<String, String> table2Db = new HashMap<>();

        for (Map<String, String> db : tables) {
            String dbName = Optional.ofNullable(db.get("TABLE_SCHEMA"))
                    .orElseThrow(() -> new StarRocksException("get table header error"));
            String tableName =
                    Optional.ofNullable(db.get("TABLE_NAME")).orElseThrow(() -> new StarRocksException("get table header error"));

            table2Db.put(tableName, dbName);
        }

        return table2Db;
    }

    private static Connection createJdbcConnection(String jdbcUrl, String username, String password) throws Exception {
        try {
            Class.forName(MYSQL_80_DRIVER_NAME);
        } catch (ClassNotFoundException e) {
            try {
                Class.forName(MYSQL_51_DRIVER_NAME);
            } catch (ClassNotFoundException ie) {
                String msg = String.format("Can't find mysql jdbc driver, please download it and "
                                + "put it in your classpath manually. Note that the connector does not include "
                                + "the mysql driver since version 1.1.1 because of the limitation of GPL license "
                                + "used by the driver. You can download it from MySQL site %s, or Maven Central %s", MYSQL_SITE_URL,
                        MAVEN_CENTRAL_URL);
                throw new StarRocksException(msg);
            }
        }

        return DriverManager.getConnection(jdbcUrl, username, password);
    }

    private static List<Map<String, String>> extractColumnValuesBySql(StarRocksConfig config, String sqlPattern,
            List<String> parameters) {
        List<Map<String, String>> columnValues = new ArrayList<>();
        try (Connection conn = createJdbcConnection(config.getFeJdbcUrl(), config.getUsername(), config.getPassword());
                PreparedStatement ps = conn.prepareStatement(sqlPattern)) {
            for (int i = 1; i <= parameters.size(); i++) {
                ps.setObject(i, parameters.get(i - 1));
            }

            ResultSet rs = ps.executeQuery();
            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();
            while (rs.next()) {
                Map<String, String> row = new HashMap<>(columnCount);
                for (int i = 1; i <= columnCount; i++) {
                    row.put(metaData.getColumnName(i), rs.getString(i));
                }
                columnValues.add(row);
            }
            rs.close();
        } catch (Exception e) {
            throw new IllegalStateException("extract column values by sql error, " + e.getMessage(), e);
        }

        if (columnValues.isEmpty()) {
            String errMsg = String.format("Can't get schema of table [%s.%s] from StarRocks. The possible reasons: "
                    + "1) The table does not exist. 2) The user does not have [SELECT] privilege on the "
                    + "table, and can't read the schema. Please make sure that the table exists in StarRocks, "
                    + "and grant [SELECT] privilege to the user. If you are loading data to the table, also need "
                    + "to grant [INSERT] privilege to the user.", config.getDatabase(), config.getTable());
            logger.error(errMsg);
        }
        return columnValues;
    }

}
