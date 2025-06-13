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
import com.starrocks.connector.spark.sql.conf.SimpleStarRocksConfig;
import com.starrocks.connector.spark.sql.conf.StarRocksConfig;
import com.starrocks.connector.spark.sql.connect.StarRocksConnector;
import com.starrocks.connector.spark.sql.schema.InferSchema;
import com.starrocks.connector.spark.sql.schema.StarRocksSchema;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableProvider;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.sources.BaseRelation;
import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.sources.RelationProvider;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;

import static com.starrocks.connector.spark.cfg.ConfigurationOptions.STARROCKS_TABLE_IDENTIFIER;
import static com.starrocks.connector.spark.cfg.ConfigurationOptions.makeWriteCompatibleWithRead;
import static com.starrocks.connector.spark.sql.StarRocksTable.identifierFullName;
import static com.starrocks.connector.spark.sql.conf.StarRocksConfig.PREFIX;

public class StarRocksDataSourceProvider implements RelationProvider,
        TableProvider,
        DataSourceRegister {

    private static final Logger LOG = LoggerFactory.getLogger(StarRocksDataSourceProvider.class);

    @Nullable
    private StarRocksSchema starrocksSchema;

    @Override
    public BaseRelation createRelation(SQLContext sqlContext,
                                       scala.collection.immutable.Map<String, String> parameters) {
        Map<String, String> mutableParams = new HashMap<>();
        scala.collection.immutable.Map<String, String> transParameters = DialectUtils.params(parameters, LOG);
        transParameters.toStream().foreach(key -> mutableParams.put(key._1, key._2));
        SimpleStarRocksConfig config = new SimpleStarRocksConfig(makeWriteCompatibleWithRead(mutableParams));
        starrocksSchema = getStarRocksSchema(config);

        return new StarRocksRelation(sqlContext, transParameters);
    }

    @Override
    public StructType inferSchema(CaseInsensitiveStringMap options) {
        SimpleStarRocksConfig config = new SimpleStarRocksConfig(makeWriteCompatibleWithRead(options));
        starrocksSchema = getStarRocksSchema(config);
        return InferSchema.inferSchema(starrocksSchema, config);
    }

    @Override
    public Table getTable(StructType schema, Transform[] partitioning, Map<String, String> properties) {
        SimpleStarRocksConfig config = new SimpleStarRocksConfig(makeWriteCompatibleWithRead(properties));
        starrocksSchema = getStarRocksSchema(config);
        return new StarRocksTable(schema, starrocksSchema, config);
    }

    @Override
    public String shortName() {
        return "starrocks";
    }

    public static StarRocksSchema getStarRocksSchema(StarRocksConfig config) {
        return getStarRocksSchema(config, null);
    }

    public static StarRocksSchema getStarRocksSchema(StarRocksConfig config, Identifier tbIdentifier) {
        PropertiesSettings properties = new PropertiesSettings();

        if (StringUtils.isEmpty(config.getDatabase())) {
            if (null == tbIdentifier || ArrayUtils.isEmpty(tbIdentifier.namespace())) {
                throw new IllegalStateException("Unknown table identifier.");
            } else {
                properties.setProperty(STARROCKS_TABLE_IDENTIFIER, identifierFullName(tbIdentifier));
            }
        }
        Map<String, String> options = makeWriteCompatibleWithRead(config.getOriginOptions());
        options.forEach(properties::setProperty);

        StarRocksSchema schema = StarRocksConnector.getSchema(config, tbIdentifier);
        return schema;
    }

    public static Map<String, String> addPrefixInStarRockConfig(Map<String, String> options) {
        Map<String, String> properties = new HashMap<>();
        options.forEach((k, v) -> {
            if (k.startsWith(PREFIX)) {
                properties.put(k, v);
            } else {
                properties.put(PREFIX + k, v);
            }
        });
        return properties;
    }
}
