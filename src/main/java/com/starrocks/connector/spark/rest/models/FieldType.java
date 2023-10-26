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

import java.util.Arrays;

public enum FieldType {
    NULL("NULL_TYPE"),
    BOOLEAN("BOOLEAN"),
    BIGINT("BIGINT"),
    INT("INT"),
    TINYINT("TINYINT"),
    SMALLINT("SMALLINT"),
    LARGEINT("LARGEINT"),
    VARCHAR("VARCHAR"),
    FLOAT("FLOAT"),
    DOUBLE("DOUBLE"),
    DATE("DATE"),
    DECIMAL("DECIMAL"),
    DECIMAL32("DECIMAL32"),
    DECIMAL64("DECIMAL64"),
    DECIMAL128("DECIMAL128"),
    DATETIME("DATETIME"),
    TIME("TIME"),
    CHAR("CHAR"),
    BINARY("BINARY"),
    DECIMALV2("DECIMALV2");

    public static FieldType of(String typeName) {
        return Arrays.stream(values())
                .filter(type ->
                        type.getTypeName().equalsIgnoreCase(typeName)
                                || type.name().equalsIgnoreCase(typeName))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("unknown field type: " + typeName));
    }

    private final String typeName;

    FieldType(String typeName) {
        this.typeName = typeName;
    }

    public String getTypeName() {
        return typeName;
    }
}
