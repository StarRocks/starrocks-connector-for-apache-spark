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
import com.starrocks.connector.spark.util.JsonUtils;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZoneId;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class JSONRowStringConverter extends AbstractRowStringConverter {

    private static final Logger LOG = LoggerFactory.getLogger(JSONRowStringConverter.class);

    private final String[] streamLoadColumnNames;
    private final boolean[] isStarRocksJsonType;
    private final ObjectMapper mapper;

    public JSONRowStringConverter(StructType schema, String[] streamLoadColumnNames, Set<String> jsonColumnNames, ZoneId timeZone) {
        super(schema, timeZone);
        this.streamLoadColumnNames = streamLoadColumnNames;
        this.mapper = JsonUtils.createObjectMapper();
        this.isStarRocksJsonType = new boolean[streamLoadColumnNames.length];
        for (int i = 0; i < streamLoadColumnNames.length; i++) {
            isStarRocksJsonType[i] = jsonColumnNames.contains(streamLoadColumnNames[i]);
        }
    }

    @Override
    public String fromRow(Row row) {
        if (row.schema() == null) {
            throw new RuntimeException("Can't convert Row without schema");
        }

        Map<String, Object> nonJsonColumnData = new HashMap<>();
        Map<String, Object> jsonColumnData = new HashMap<>();
        for (int i = 0; i < row.length(); i++) {
            StructField field = row.schema().apply(i);
            if (!(field.nullable() && row.isNullAt(i))) {
                if (!isStarRocksJsonType[i]) {
                    nonJsonColumnData.put(streamLoadColumnNames[i], valueConverters[i].apply(row.get(i)));
                } else {
                    jsonColumnData.put(streamLoadColumnNames[i], valueConverters[i].apply(row.get(i)));
                }
            }
        }

        try {
            String result = mapper.writeValueAsString(nonJsonColumnData);
            if (!jsonColumnData.isEmpty()) {
                StringBuilder builder = new StringBuilder();
                builder.append(result, 0, result.length() - 1);
                for (Map.Entry<String, Object> entry : jsonColumnData.entrySet()) {
                    builder.append(",\"");
                    builder.append(entry.getKey());
                    builder.append("\":");
                    builder.append(entry.getValue());
                }
                builder.append("}");
                result = builder.toString();
            }
            return result;
        } catch (Exception e) {
            LOG.error("Failed to serialize row to json, data: {}", nonJsonColumnData, e);
            throw new RuntimeException(e);
        }
    }
}
