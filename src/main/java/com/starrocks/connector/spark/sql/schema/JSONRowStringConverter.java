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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZoneId;
import java.util.HashMap;
import java.util.Map;

public class JSONRowStringConverter extends AbstractRowStringConverter {

    private static final Logger LOG = LoggerFactory.getLogger(JSONRowStringConverter.class);

    private final ObjectMapper mapper;

    public JSONRowStringConverter(StructType schema, ZoneId timeZone) {
        super(schema, timeZone);
        this.mapper = new ObjectMapper();
    }

    @Override
    public String fromRow(Row row) {
        if (row.schema() == null) {
            throw new RuntimeException("Can't convert Row without schema");
        }

        Map<String, Object> data = new HashMap<>();
        for (StructField field : row.schema().fields()) {
            int idx = row.fieldIndex(field.name());
            if (!(field.nullable() && row.isNullAt(idx))) {
                data.put(field.name(), convert(field.dataType(), row.get(idx)));
            }
        }

        try {
            return mapper.writeValueAsString(data);
        } catch (Exception e) {
            LOG.error("Failed to serialize row to json, data: {}", data, e);
            throw new RuntimeException(e);
        }
    }
}
