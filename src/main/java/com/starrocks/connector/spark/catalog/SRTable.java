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


import javax.annotation.Nullable;
import java.util.*;

//to describe a column of SR,info from information_schema.tables

public class SRTable {


    public enum TableType {
        UNKNOWN,
        DUPLICATE_KEYS,
        AGGREGATES,
        UNIQUE_KEYS,
        PRIMARY_KEYS
    }
    //database's name
    private final String databaseName;
    //table's name
    private final String tableName;
    //SR table's type only primary can use stageMode
    private final TableType tableType;
    //columns describe
    private final List<SRColumn> columns;
    //table's primaryKey for pks table, can more than one
    @Nullable
    private final List<String> tableKeys;
    //table properties
    private final Map<String, String> properties;
    @Nullable
    private final List<String> distributionKeys;
    @Nullable
    private final Integer numBuckets;
    //comment about table
    @Nullable private final String comment;
    private SRTable(
            String databaseName,
            String tableName,
            TableType tableType,
            List<SRColumn> columns,
            @Nullable List<String> tableKeys,
            @Nullable List<String> distributionKeys,
            @Nullable Integer numBuckets,
            @Nullable String comment,
            Map<String, String> properties) {
        Preconditions.checkNotNull(databaseName);
        Preconditions.checkNotNull(tableName);
        Preconditions.checkNotNull(tableType);
        Preconditions.checkArgument(columns != null && !columns.isEmpty());
        this.databaseName = databaseName;
        this.tableName = tableName;
        this.tableType = tableType;
        this.columns = columns;
        this.tableKeys = tableKeys;
        this.distributionKeys = distributionKeys;
        this.numBuckets = numBuckets;
        this.comment = comment;
        this.properties = Preconditions.checkNotNull(properties);
    }
    public String getDatabaseName() {
        return databaseName;
    }
    public String getTableName() {
        return tableName;
    }
    public TableType getTableType() {
        return tableType;
    }
    public List<SRColumn> getColumns() {
        return columns;
    }

    public Optional<List<String>> getTableKeys() {
        return Optional.ofNullable(tableKeys);
    }
    public Optional<List<String>> getDistributionKeys() {
        return Optional.ofNullable(distributionKeys);
    }
    public Optional<Integer> getNumBuckets() {
        return Optional.ofNullable(numBuckets);
    }
    public Optional<String> getComment() {
        return Optional.ofNullable(comment);
    }
    public Map<String, String> getProperties() {
        return properties;
    }

    @Override
    public String toString() {
        return "StarRocksTable{" +
                "databaseName='" + databaseName + '\'' +
                ", tableName='" + tableName + '\'' +
                ", tableType=" + tableType +
                ", columns=" + columns +
                ", tableKeys=" + tableKeys +
                ", comment='" + comment + '\'' +
                ", properties=" + properties +
                '}';
    }
    public static class Builder {
        private String databaseName;
        private String tableName;
        private TableType tableType;
        private List<SRColumn> columns = new ArrayList<>();
        private List<String> tableKeys;
        private List<String> distributionKeys;
        private Integer numBuckets;
        private String comment;
        private Map<String, String> properties = new HashMap<>();
        public Builder setDatabaseName(String databaseName) {
            this.databaseName = databaseName;
            return this;
        }
        public Builder setTableName(String tableName) {
            this.tableName = tableName;
            return this;
        }
        public Builder setTableType(String tableType) {
            switch (tableType) {
                case "DUPLICATE_KEYS" :
                    this.tableType = TableType.DUPLICATE_KEYS;
                    break;
                case "AGGREGATES":
                    this.tableType = TableType.AGGREGATES;
                    break;
                case "UNIQUE_KEYS":
                    this.tableType = TableType.UNIQUE_KEYS;
                    break;
                case "PRIMARY_KEYS":
                    this.tableType = TableType.PRIMARY_KEYS;
                    break;
                default:
                   this.tableType = TableType.UNKNOWN;
            }
            return this;
        }
        public Builder setTableType(TableType tableType)
        {
            this.tableType = tableType;
            return this;
        }
        public Builder setColumns(List<SRColumn> columns) {
            this.columns = columns;
            return this;
        }
        public Builder setTableKeys(List<String> tableKeys) {
            this.tableKeys = tableKeys;
            return this;
        }
        public Builder setDistributionKeys(List<String> distributionKeys) {
            this.distributionKeys = distributionKeys;
            return this;
        }
        public Builder setNumBuckets(Integer numBuckets) {
            this.numBuckets = numBuckets;
            return this;
        }
        public Builder setComment(String comment) {
            this.comment = comment;
            return this;
        }
        public Builder setTableProperties(Map<String, String> properties) {
            this.properties = properties;
            return this;
        }
        public SRTable build() {
            return new SRTable(
                    databaseName,
                    tableName,
                    tableType,
                    columns,
                    tableKeys,
                    distributionKeys,
                    numBuckets,
                    comment,
                    properties);
        }
    }

}
