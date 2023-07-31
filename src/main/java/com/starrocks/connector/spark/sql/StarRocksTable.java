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

package com.starrocks.connector.spark.sql;

import com.starrocks.connector.spark.sql.conf.StarRocksConfig;
import com.starrocks.connector.spark.sql.conf.WriteStarRocksConfig;
import com.starrocks.connector.spark.sql.schema.StarRocksSchema;
import com.starrocks.connector.spark.sql.write.StarRocksWriteBuilder;
import org.apache.spark.sql.connector.catalog.SupportsWrite;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class StarRocksTable implements Table, SupportsWrite {

    private static final Set<TableCapability> TABLE_CAPABILITY_SET = Collections.unmodifiableSet(
            new HashSet<>(
                    Arrays.asList(
                            TableCapability.BATCH_WRITE,
                            TableCapability.STREAMING_WRITE
                    )
            )
    );

    private final StructType schema;
    private final StarRocksSchema starRocksSchema;
    private final StarRocksConfig config;

    public StarRocksTable(StructType schema, StarRocksSchema starRocksSchema, StarRocksConfig config) {
        this.schema = schema;
        this.starRocksSchema = starRocksSchema;
        this.config = config;
    }

    @Override
    public WriteBuilder newWriteBuilder(LogicalWriteInfo info) {
        WriteStarRocksConfig writeConfig = new WriteStarRocksConfig(config.getOriginOptions(), schema, starRocksSchema);
        return new StarRocksWriteBuilder(info, writeConfig);
    }

    @Override
    public String name() {
        return String.format("StarRocksTable[%s.%s]", config.getDatabase(), config.getTable());
    }

    @Override
    public StructType schema() {
        return schema;
    }

    @Override
    public Set<TableCapability> capabilities() {
        return TABLE_CAPABILITY_SET;
    }
}
