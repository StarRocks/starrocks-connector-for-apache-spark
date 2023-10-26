// Modifications Copyright 2021 StarRocks Limited.
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

package com.starrocks.connector.spark.rest.models;

import java.io.Serializable;
import java.util.Objects;
import java.util.Optional;
import javax.annotation.Nullable;

public class Field implements Serializable {
    private String name;
    @Nullable
    private String type;
    private String comment;
    private Integer precision;
    private Integer scale;
    private String isKey;
    private Integer columnSize;

    public Field() {
    }

    public Field(String name,
                 @Nullable String type,
                 Integer columnSize,
                 String comment,
                 Integer precision,
                 Integer scale,
                 boolean isKey) {
        this(name, type, columnSize, comment, precision, scale, isKey ? "true" : "false");
    }

    public Field(String name,
                 @Nullable String type,
                 Integer columnSize,
                 String comment,
                 Integer precision,
                 Integer scale,
                 String isKey) {
        this.name = name;
        this.type = type;
        this.columnSize = columnSize;
        this.comment = comment;
        this.precision = precision;
        this.scale = scale;
        this.isKey = isKey;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Optional<String> getType() {
        return Optional.ofNullable(type);
    }

    public void setType(@Nullable String type) {
        this.type = type;
    }

    public Integer getColumnSize() {
        return columnSize;
    }

    public void setColumnSize(Integer columnSize) {
        this.columnSize = columnSize;
    }

    public String getComment() {
        return comment;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    public Integer getPrecision() {
        return precision;
    }

    public void setPrecision(Integer precision) {
        this.precision = precision;
    }

    public Integer getScale() {
        return scale;
    }

    public void setScale(Integer scale) {
        this.scale = scale;
    }

    public boolean getIsKey() {
        return "true".equalsIgnoreCase(isKey);
    }

    public void setIsKey(String isKey) {
        this.isKey = isKey;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Field field = (Field) o;
        return Objects.equals(precision, field.precision) &&
                Objects.equals(scale, field.scale) &&
                Objects.equals(name, field.name) &&
                Objects.equals(type, field.type) &&
                Objects.equals(comment, field.comment) &&
                Objects.equals(isKey, field.isKey) &&
                Objects.equals(columnSize, field.columnSize);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, type, columnSize, comment, precision, scale, isKey);
    }

    @Override
    public String toString() {
        return "Field{" +
                "name='" + name + '\'' +
                ", type='" + type + '\'' +
                ", columnSize=" + columnSize +
                ", comment='" + comment + '\'' +
                ", precision=" + precision +
                ", scale=" + scale +
                ", isKey=" + isKey +
                '}';
    }
}
