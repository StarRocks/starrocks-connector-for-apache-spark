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

import com.starrocks.connector.spark.catalog.SRColumn;
import com.starrocks.connector.spark.catalog.SRTable;
import com.starrocks.connector.spark.exception.CatalogException;
import com.starrocks.connector.spark.exception.StarRocksException;
import com.starrocks.connector.spark.sql.conf.StarRocksConfig;
import com.starrocks.connector.spark.sql.schema.StarRocksField;
import com.starrocks.connector.spark.sql.schema.StarRocksSchema;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.*;

public class StarRocksConnector {
    private static final Logger logger = LoggerFactory.getLogger(StarRocksConnector.class);

    private static final String TABLE_SCHEMA_QUERY =
            "SELECT `COLUMN_NAME`, `ORDINAL_POSITION`, `COLUMN_KEY`, `DATA_TYPE`, `COLUMN_SIZE`, `DECIMAL_DIGITS` "
                    + "FROM `information_schema`.`COLUMNS` WHERE `TABLE_SCHEMA`=? AND `TABLE_NAME`=?;";
    private static final String ALL_DBS_QUERY = "show databases;";

    private static final String LOAD_DB_QUERY =
            "select SCHEMA_NAME from information_schema.schemata where SCHEMA_NAME in (?) AND CATALOG_NAME = 'def';";

    private static final String ALL_TABLES_QUERY = "select TABLE_SCHEMA, TABLE_NAME from information_schema.tables "
            + "where TABLE_TYPE = 'BASE TABLE' AND TABLE_SCHEMA in (?) ;";
    private static final String DB_EXISTS =
            "select SCHEMA_NAME from information_schema.schemata where SCHEMA_NAME =? AND CATALOG_NAME = 'def';";
    private static final String Table_COLUMNS_QUERY=
            "select COLUMN_NAME,ORDINAL_POSITION,DATA_TYPE,COLUMN_SIZE,DECIMAL_DIGITS,COLUMN_DEFAULT,IS_NULLABLE,COLUMN_COMMENT from INFORMATION_SCHEMA.columns where TABLE_SCHEMA=? and TABLE_NAME=?";
    private static final String TABLES_QUERY =
            "select TABLE_SCHEMA,TABLE_NAME,TABLE_COMMENT from INFORMATION_SCHEMA.tables where TABLE_SCHEMA=? AND TABLE_NAME = ?";
    private static final String TABLE_CONFIG_QUERY="select TABLE_MODEL,PRIMARY_KEY,DISTRIBUTE_KEY,DISTRIBUTE_BUCKET,PROPERTIES from INFORMATION_SCHEMA.tables_config where TABLE_SCHEMA=? AND TABLE_NAME = ?";
    // Driver name for mysql connector 5.1 which is deprecated in 8.0
    private static final String MYSQL_51_DRIVER_NAME = "com.mysql.jdbc.Driver";
    // Driver name for mysql connector 8.0
    private static final String MYSQL_80_DRIVER_NAME = "com.mysql.cj.jdbc.Driver";
    private static final String MYSQL_SITE_URL = "https://dev.mysql.com/downloads/connector/j/";
    private static final String MAVEN_CENTRAL_URL = "https://repo1.maven.org/maven2/mysql/mysql-connector-java/";

    public static StarRocksSchema getSchema(StarRocksConfig config) {
        List<String> parameters = Arrays.asList(config.getDatabase(), config.getTable());
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
//only get tables of one db
    public static Map<String, String> getTables(StarRocksConfig config, List<String> dbNames) {
        List<String> parameters = Arrays.asList(java.lang.String.join(",", dbNames));
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

public static boolean dbExists(StarRocksConfig config, String dbName) {

    List<Map<String, String>> res = extractColumnValuesBySql(config, DB_EXISTS, Arrays.asList(dbName));
    return !res.isEmpty();
}


 public static  SRTable getSRTable(StarRocksConfig config, String dbName, String tableName) {

     Map<String, String> tableConfig = getTableConfig(config, dbName, tableName);
     List<SRColumn> srColumns = getSRColumns(config, dbName, tableName);
     List<Map<String, String>> maps = extractColumnValuesBySql(config, TABLES_QUERY, Arrays.asList(dbName, tableName));
     if (maps.isEmpty()) {
         throw new StarRocksException("table does not exist: " + dbName + "." + tableName);
     }

     Map<String, String> tables = maps.get(0);
     String db_name = tables.get("TABLE_SCHEMA");
     String table_name = tables.get("TABLE_NAME");
     String table_comment = tables.get("TABLE_COMMENT");
     String table_type = tableConfig.get("TABLE_MODEL");
     String[] arr = tableConfig.get("PRIMARY_KEY").replace("`","").split(",");
     String[] trimmedArr = Arrays.stream(arr)
             .map(String::trim)
             .toArray(String[]::new);
     List<String> primary_key = Arrays.asList(trimmedArr);
     int num_buckets = Integer.parseInt(tableConfig.get("DISTRIBUTE_BUCKET"));
     String[] distributeKeys = tableConfig.get("DISTRIBUTE_KEY").replace("`","").split(",");
     String[] trimmedDistributeKeys = Arrays.stream(distributeKeys)
             .map(String::trim)
             .toArray(String[]::new);
     List<String> distribution_keys = Arrays.asList(trimmedDistributeKeys);

     String proper = tableConfig.get("PROPERTIES");
    proper=proper.trim().replaceAll("[{}]","");
     String[] entries = proper.split(",");
     Map<String, String> table_properties = new HashMap<>();
     for (String entry : entries) {
         String[] keyValue = entry.split(":");
         table_properties.put(keyValue[0].replace("\"", "").trim(), keyValue[1].replace("\"", "").trim());
     }
     SRTable res=new SRTable.Builder()
             .setTableName(table_name)
             .setComment(table_comment)
             .setColumns(srColumns)
             .setTableKeys(primary_key)
             .setDatabaseName(db_name)
             .setNumBuckets(num_buckets)
             .setTableType(table_type)
             .setTableProperties(table_properties)
             .setDistributionKeys(distribution_keys)
             .build();
     return res;
 }

    public static Map<String, String> getTableConfig(StarRocksConfig config, String dbName, String tableName) {
        List<Map<String, String>> res = extractColumnValuesBySql(config, TABLE_CONFIG_QUERY, Arrays.asList(dbName, tableName));
        if (!res.isEmpty()) {
            return res.get(0);
        }
        else
        {
            throw new StarRocksException("table does not exist: " + dbName + "." + tableName);
        }
    }

    public static List<SRColumn> getSRColumns(StarRocksConfig config, String dbName, String tableName) {
     ArrayList<SRColumn> srColumns = new ArrayList<>();
     List<Map<String, String>> res = extractColumnValuesBySql(config, Table_COLUMNS_QUERY, Arrays.asList(dbName, tableName));
     if (!res.isEmpty()) {

         for (Map<String, String> columnMap : res) {
             String columnSize = columnMap.get("COLUMN_SIZE");
             Integer size=null;
             if (columnSize != null) {
                  size = Integer.parseInt(columnSize);
             }
             SRColumn column = new SRColumn.Builder()
                     .setColumnName(columnMap.get("COLUMN_NAME"))
                     .setOrdinalPosition(Integer.parseInt(columnMap.get("ORDINAL_POSITION")))
                     .setDataType(columnMap.get("DATA_TYPE"),size)
                     .setNullable(columnMap.get("IS_NULLABLE"))
                     .setDefaultValue(columnMap.get("COLUMN_DEFAULT"))
                     .setColumnSize(size)
                     .setDecimalDigits((columnMap.get("DECIMAL_DIGITS")))
                     .setColumnComment(columnMap.get("COLUMN_COMMENT"))
                     .build();
             srColumns.add(column);
         }
     }
     else
     {
         throw new CatalogException("table does not exist: " + dbName + "." + tableName);
     }
     return srColumns;
 }

    public static Connection createJdbcConnection(String jdbcUrl, String username, String password) throws Exception {
        try {
            Class.forName(MYSQL_80_DRIVER_NAME);
        } catch (ClassNotFoundException e) {
            try {
                Class.forName(MYSQL_51_DRIVER_NAME);
            } catch (ClassNotFoundException ie) {
                String msg = java.lang.String.format("Can't find mysql jdbc driver, please download it and "
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
            String errMsg = java.lang.String.format("Can't get schema of table [%s.%s] from StarRocks. The possible reasons: "
                    + "1) The table does not exist. 2) The user does not have [SELECT] privilege on the "
                    + "table, and can't read the schema. Please make sure that the table exists in StarRocks, "
                    + "and grant [SELECT] privilege to the user. If you are loading data to the table, also need "
                    + "to grant [INSERT] privilege to the user.", config.getDatabase(), config.getTable());
            logger.error(errMsg);
        }
        return columnValues;
    }

}
