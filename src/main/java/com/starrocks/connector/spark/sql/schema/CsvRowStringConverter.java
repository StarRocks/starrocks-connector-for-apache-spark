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

import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;

import java.time.ZoneId;

public class CsvRowStringConverter extends AbstractRowStringConverter {

    private final String separator;

    public CsvRowStringConverter(StructType schema, String separator, ZoneId timeZone) {
        super(schema, timeZone);
        this.separator = separator;
    }

    @Override
    public String fromRow(Row row) {
        if (row.schema() == null) {
            throw new RuntimeException("Can't convert Row without schema");
        }
        String[] data = new String[row.length()];
        for (int i = 0; i < row.length(); i++) {
            if (!row.isNullAt(i)) {
                data[i] = valueConverters[i].apply(row.get(i)).toString();
            }
        }

        StringBuilder sb = new StringBuilder();
        for (int idx = 0; idx < data.length; idx++) {
            Object val = data[idx];
            sb.append(null == val ? "\\N" : val);
            if (idx < data.length - 1) {
                sb.append(separator);
            }
        }
        return sb.toString();
    }
}
