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

package com.starrocks.connector.spark.sql.schema;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.starrocks.connector.spark.sql.schema.StarRocksField.__OP;

public class StarRocksSchema {
    private final List<StarRocksField> columns;
    private final List<StarRocksField> pks;
    private final Map<String, StarRocksField> columnMap;

    public StarRocksSchema(List<StarRocksField> columns, List<StarRocksField> pks) {
        this.columns = columns;
        this.pks = pks;
        this.columnMap = new HashMap<>();
        for (StarRocksField field : columns) {
            columnMap.put(field.getName(), field);
        }
    }

    public List<StarRocksField> getColumns() {
        return columns;
    }

    public boolean isPrimaryKey() {
        return !pks.isEmpty();
    }

    public StarRocksField getField(String columnName) {
        if (__OP.getName().equalsIgnoreCase(columnName)) {
            return __OP;
        }

        return columnMap.get(columnName);
    }
}
