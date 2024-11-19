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
import java.io.Serializable;
import java.util.Objects;
import java.util.Optional;
//to describe a column of SR, info from information_schema.COLUMNS
public class SRColumn implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String columnName;
    //column's position
    private final int ordinalPosition;
    //column type
    private final String dataType;
    //can be null or not
    private final boolean isNullable;
    // column's defaultValue
    @Nullable
    private final String defaultValue;
    //column' maxSize
    @Nullable
    private final Integer columnSize;
    //decimal 0 0.1 null if not a number
    @Nullable
    private final Integer decimalDigits;
    //comment about a column
    @Nullable
    private final String columnComment;
    private SRColumn(
            String columnName,
            int ordinalPosition,
            String dataType,
            boolean isNullable,
            @Nullable String defaultValue,
            @Nullable Integer columnSize,
            @Nullable Integer decimalDigits,
            @Nullable String columnComment) {
        this.columnName = Preconditions.checkNotNull(columnName);
        this.ordinalPosition = ordinalPosition;
        this.dataType = Preconditions.checkNotNull(dataType);
        this.isNullable = isNullable;
        this.defaultValue = defaultValue;
        this.columnSize = columnSize;
        this.decimalDigits = decimalDigits;
        this.columnComment = columnComment;
    }
    public String getColumnName() {
        return columnName;
    }
    public int getOrdinalPosition() {
        return ordinalPosition;
    }
    public String getDataType() {
        return dataType;
    }
    public boolean isNullable() {
        return isNullable;
    }
    public Optional<String> getDefaultValue() {
        return Optional.ofNullable(defaultValue);
    }
    public Optional<Integer> getColumnSize() {
        return Optional.ofNullable(columnSize);
    }
    public Optional<Integer> getDecimalDigits() {
        return Optional.ofNullable(decimalDigits);
    }
    public Optional<String> getColumnComment() {
        return Optional.ofNullable(columnComment);
    }
    @Override
    public String toString() {
        return "StarRocksColumn{" +
                "columnName='" + columnName + '\'' +
                ", ordinalPosition=" + ordinalPosition +
                ", dataType='" + dataType + '\'' +
                ", isNullable=" + isNullable +
                ", defaultValue='" + defaultValue + '\'' +
                ", columnSize=" + columnSize +
                ", decimalDigits=" + decimalDigits +
                ", columnComment='" + columnComment + '\'' +
                '}';
    }
    public static class Builder {
        private String columnName;
        private int ordinalPosition;
        private String dataType;
        private boolean isNullable = true;
        private String defaultValue;
        private Integer columnSize;
        private Integer decimalDigits;
        private String columnComment;
        public SRColumn.Builder setColumnName(String columnName) {
            this.columnName = columnName;
            return this;
        }
        public SRColumn.Builder setOrdinalPosition(int ordinalPosition) {
            this.ordinalPosition = ordinalPosition;
            return this;
        }
        public SRColumn.Builder setDataType(String dataType,Integer size) {
            if ("tinyint".equalsIgnoreCase(dataType) && size == null) {
                this.dataType = "boolean";
            }
            else {
                this.dataType = dataType;
            }

            return this;
        }
        public SRColumn.Builder setDataType(String dataType) {
                this.dataType = dataType;
            return this;
        }
        public SRColumn.Builder setNullable(String isNULL) {

            if ("YES".equalsIgnoreCase(isNULL)) {
                this.isNullable = true;
            }
            else
            {
                this.isNullable = false;
            }
            return this;
        }
        public SRColumn.Builder setDefaultValue(String defaultValue) {
            this.defaultValue = defaultValue;
            return this;
        }
        public SRColumn.Builder setColumnSize(Integer columnSize) {
            this.columnSize = columnSize;
            return this;
        }
        public SRColumn.Builder setDecimalDigits(String decimalDigits) {
            if (Objects.isNull(decimalDigits)) {
                this.decimalDigits = null;
            }
            else
            {
                this.decimalDigits = Integer.parseInt(decimalDigits);
            }
            return this;
        }

        public SRColumn.Builder setColumnComment(String columnComment) {
            this.columnComment = columnComment;
            return this;
        }
        public SRColumn build() {
            return new SRColumn(
                    columnName,
                    ordinalPosition,
                    dataType,
                    isNullable,
                    defaultValue,
                    columnSize,
                    decimalDigits,
                    columnComment);
        }
    }
}
