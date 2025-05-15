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
package com.starrocks.connector.spark.catalog;

import com.google.common.base.Preconditions;
import com.starrocks.connector.spark.exception.CatalogException;
import com.starrocks.connector.spark.sql.connect.StarRocksConnector;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class SRCatalog implements Serializable {
//supply someBase sql Method for BatchWrite StageMode

private static final long serialVersionUID = 1L;

private static final Logger LOG = LoggerFactory.getLogger(StarRocksCatalog.class);

    private final String jdbcUrl;
    private final String username;
    private final String password;
    public SRCatalog(String jdbcUrl, String username, String password) {
        this.jdbcUrl = jdbcUrl;
        this.username = username;
        this.password = password;
    }
    public Connection getJdbcConnection() throws Exception {
        Connection jdbcConnection = StarRocksConnector.createJdbcConnection(jdbcUrl, username, password);
        return jdbcConnection;
    }

public void executeDDLBySql(String ddlSql) throws Exception {
       try (Connection connection = getJdbcConnection()) {
           Statement statement = connection.createStatement();
           statement.execute(ddlSql);
       }
       catch (Exception e) {
           throw new IllegalStateException("execute ddl error, " + e.getMessage(), e);
       }
}

    //create db if not exists
    public  void createDatabase (String dbNames,boolean ignoreIfExists)  {
        if (StringUtils.isEmpty(dbNames)) {
            throw new CatalogException("create db error");
        }
        try {
            String buildSql = buildCreateDatabaseSql(dbNames, ignoreIfExists);
            executeDDLBySql(buildSql);
        }
        catch (Exception e) {

            throw new CatalogException("create db error", e);
        }
        LOG.info("successfully created database {}", dbNames);
    }

    public void createTable(SRTable table, boolean ignoreIfExists)
    {
       try {
           String sql = buildCreateTableSql(table, ignoreIfExists);
           executeDDLBySql(sql);
       }
       catch (Exception e) {
           throw new CatalogException("create table error", e);
       }
       LOG.info("successfully created {}", table);
    }

    public void  dropTable(String dbName,String tableName)  {
        String sql = String.format(
                "SELECT TABLE_NAME FROM information_schema.tables WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME = '%s'",
                dbName, tableName
        );
        try (Connection connection = getJdbcConnection(); Statement stmt = connection.createStatement()) {
            ResultSet resultSet = stmt.executeQuery(sql);
          if (resultSet.next()) {
              stmt.executeUpdate("DROP TABLE `" + dbName + "`.`" + tableName + "`");
          }
        }
catch (Exception e) {
            throw new CatalogException("drop table error", e);
}
        LOG.info("successfully dropped {}", tableName);
    }

    private String buildCreateDatabaseSql(String databaseName, boolean ignoreIfExists) {
        StringBuilder sql = new StringBuilder("CREATE DATABASE ");
        if (ignoreIfExists) {
            sql.append("IF NOT EXISTS ");
        }
        sql.append(databaseName);
        return sql.toString();
    }

    private String buildCreateTableSql(SRTable table, boolean ignoreIfExists) {
        StringBuilder builder = new StringBuilder();
        builder.append(
                String.format(
                        "CREATE TABLE %s`%s`.`%s`",
                        ignoreIfExists ? "IF NOT EXISTS " : "",
                        table.getDatabaseName(),
                        table.getTableName()));
        builder.append(" (\n");

        List<String> tableKeys = table.getTableKeys().orElse(Collections.emptyList());

        String columnsStmt =
                table.getColumns().stream()
                        .map(column -> buildColumnStmt(column, tableKeys))
                        .collect(Collectors.joining(",\n"));

        builder.append(columnsStmt);
        builder.append("\n) ");

        String keyModel;
        switch (table.getTableType()) {
            case DUPLICATE_KEYS:
                keyModel = "DUPLICATE KEY";
                break;
            case PRIMARY_KEYS:
                keyModel = "PRIMARY KEY";
                break;
            case UNIQUE_KEYS:
                keyModel = "UNIQUE KEY";
                break;
            default:
                throw new UnsupportedOperationException(
                        "Not support to build create table sql for table type " + table.getTableType());
        }

        builder.append(String.format("%s (%s)\n", keyModel, String.join(", ", tableKeys)));

        if (!table.getDistributionKeys().isPresent()) {
            Preconditions.checkArgument(
                    table.getTableType() == SRTable.TableType.DUPLICATE_KEYS,
                    "Can't build create table sql because there is no distribution keys");
        } else {
            String distributionKeys =
                    table.getDistributionKeys().get().stream()
                            .map(key -> "`" + key + "`")
                            .collect(Collectors.joining(", "));
            builder.append(String.format("DISTRIBUTED BY HASH (%s)", distributionKeys));
        }

        if (table.getNumBuckets().isPresent()) {
            builder.append(" BUCKETS ");
            builder.append(table.getNumBuckets().get());
        }

        if (!table.getProperties().isEmpty()) {
            builder.append("\nPROPERTIES (\n");
            String properties =
                    table.getProperties().entrySet().stream()
                            .map(entry -> String.format("\"%s\" = \"%s\"", entry.getKey(), entry.getValue()))
                            .collect(Collectors.joining(",\n"));
            builder.append(properties);
            builder.append("\n)");
        }

        builder.append(";");
        return builder.toString();
    }

    private String buildColumnStmt(SRColumn column, List<String> tableKeys) {
        StringBuilder builder = new StringBuilder();
        builder.append("`");
        builder.append(column.getColumnName());
        builder.append("` ");

        builder.append(
                getFullColumnType(
                        column.getDataType(), column.getColumnSize(), column.getDecimalDigits()));
        builder.append(" ");

        if (tableKeys.contains(column.getColumnName())) {
            builder.append("NOT NULL");
        } else {
            builder.append(column.isNullable() ? "NULL" : "NOT NULL");
        }

        if (column.getDefaultValue().isPresent()) {
            builder.append(String.format(" DEFAULT \"%s\"", column.getDefaultValue().get()));
        }

        if (column.getColumnComment().isPresent()) {
            builder.append(String.format(" COMMENT \"%s\"", column.getColumnComment().get()));
        }

        return builder.toString();
    }

    private String getFullColumnType(
            String type, Optional<Integer> columnSize, Optional<Integer> decimalDigits) {
        String dataType = type.toUpperCase();
        switch (dataType) {
            case "DECIMAL":
                Preconditions.checkArgument(
                        columnSize.isPresent(), "DECIMAL type must have column size");
                Preconditions.checkArgument(
                        decimalDigits.isPresent(), "DECIMAL type must have decimal digits");
                return String.format("DECIMAL(%d, %s)", columnSize.get(), decimalDigits.get());
            case "CHAR":
            case "VARCHAR":
                Preconditions.checkArgument(
                        columnSize.isPresent(), type + " type must have column size");
                return String.format("%s(%d)", dataType, columnSize.get());
            default:
                return dataType;
        }
    }
}
