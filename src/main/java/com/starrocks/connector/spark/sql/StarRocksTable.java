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

import com.starrocks.connector.spark.cfg.PropertiesSettings;
import com.starrocks.connector.spark.exception.StarRocksException;
import com.starrocks.connector.spark.read.StarRocksScanBuilder;
import com.starrocks.connector.spark.sql.conf.StarRocksConfig;
import com.starrocks.connector.spark.sql.conf.WriteStarRocksConfig;
import com.starrocks.connector.spark.sql.schema.StarRocksSchema;
import com.starrocks.connector.spark.sql.write.StarRocksWriteBuilder;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.connector.catalog.SupportsWrite;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static com.starrocks.connector.spark.cfg.ConfigurationOptions.STARROCKS_TABLE_IDENTIFIER;
import static com.starrocks.connector.spark.cfg.ConfigurationOptions.makeWriteCompatibleWithRead;

public class StarRocksTable implements Table, SupportsWrite, SupportsRead {
    private final StructType schema;
    private final StarRocksSchema starRocksSchema;
    private final StarRocksConfig config;
    private Identifier identifier;

    public StarRocksTable(StructType schema,
                          StarRocksSchema starRocksSchema,
                          StarRocksConfig config,
                          Identifier identifier) {
        this.schema = schema;
        this.starRocksSchema = starRocksSchema;
        this.config = config;
        this.identifier = identifier;
    }

    public StarRocksTable(StructType schema,
                          StarRocksSchema starRocksSchema,
                          StarRocksConfig config) {
        this.schema = schema;
        this.starRocksSchema = starRocksSchema;
        this.config = config;
    }

    @Override
    public WriteBuilder newWriteBuilder(LogicalWriteInfo info) {
        PropertiesSettings properties = addTableInfoForOptions(config.getOriginOptions());
        WriteStarRocksConfig writeConfig = new WriteStarRocksConfig(properties.getPropertyMap(), schema, starRocksSchema);
        checkWriteParameter(writeConfig);

        return new StarRocksWriteBuilder(info, writeConfig, starRocksSchema);
    }

    @Override
    public String name() {
        return String.format("StarRocksTable[%s]", identifierFullName(identifier));
    }

    @Override
    public StructType schema() {
        return schema;
    }

    @Override
    public Set<TableCapability> capabilities() {
        return TABLE_CAPABILITY_SET;
    }

    private void checkWriteParameter(WriteStarRocksConfig config) {
        if (!starRocksSchema.isPrimaryKey() && config.isPartialUpdate()) {
            String errMsg = String.format(
                    "Table %s.%s is not a primary key table, and not supports partial update. " +
                            "You could create a primary key table, " +
                            "or remove the option 'starrocks.write.properties.partial_update'.",
                    config.getDatabase(), config.getTable());
            throw new StarRocksException(errMsg);
        }
    }

    private static final Set<TableCapability> TABLE_CAPABILITY_SET = new HashSet<>(
            Arrays.asList(TableCapability.BATCH_READ, TableCapability.BATCH_WRITE, TableCapability.STREAMING_WRITE));

    @Override
    public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
        PropertiesSettings properties = addTableInfoForOptions(options.asCaseSensitiveMap());
        return new StarRocksScanBuilder(identifierFullName(identifier), schema, starRocksSchema, properties);
    }

    public static String identifierFullName(Identifier identifier) {
        return null != identifier ? String.format("%s.%s", identifier.namespace()[0], identifier.name()) : "";
    }

    private PropertiesSettings addTableInfoForOptions(Map<String, String> options) {
        PropertiesSettings properties = new PropertiesSettings();
        options.forEach(properties::setProperty);
        Map<String, String> configMap = makeWriteCompatibleWithRead(config.getOriginOptions());
        configMap.forEach(properties::setProperty);
        if (properties.getProperty(STARROCKS_TABLE_IDENTIFIER) == null) {
            properties.setProperty(STARROCKS_TABLE_IDENTIFIER, identifierFullName(identifier));
        }
        return properties;
    }
}
