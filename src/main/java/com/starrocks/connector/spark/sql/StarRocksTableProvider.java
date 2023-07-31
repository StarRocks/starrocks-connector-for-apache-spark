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

import com.starrocks.connector.spark.sql.conf.SimpleStarRocksConfig;
import com.starrocks.connector.spark.sql.conf.StarRocksConfig;
import com.starrocks.connector.spark.sql.connect.StarRocksConnector;
import com.starrocks.connector.spark.sql.schema.InferSchema;
import com.starrocks.connector.spark.sql.schema.StarRocksSchema;
import org.apache.spark.sql.SQLContext;
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

import static com.starrocks.connector.spark.cfg.ConfigurationOptions.STARROCKS_FENODES;
import static com.starrocks.connector.spark.cfg.ConfigurationOptions.STARROCKS_PASSWORD;
import static com.starrocks.connector.spark.cfg.ConfigurationOptions.STARROCKS_USER;
import static com.starrocks.connector.spark.sql.conf.StarRocksConfigBase.KEY_FE_HTTP;

public class StarRocksTableProvider implements RelationProvider, TableProvider, DataSourceRegister {

    private static final Logger LOG = LoggerFactory.getLogger(StarRocksTableProvider.class);

    @Nullable
    private StarRocksSchema starocksSchema;

    @Override
    public BaseRelation createRelation(SQLContext sqlContext,
                                       scala.collection.immutable.Map<String, String> parameters) {
        return new StarrocksRelation(sqlContext, Utils.params(parameters, LOG));
    }

    @Override
    public StructType inferSchema(CaseInsensitiveStringMap options) {
        SimpleStarRocksConfig config = new SimpleStarRocksConfig(makeWriteCompatibleWithRead(options));
        starocksSchema = getStarRocksSchema(config);
        return InferSchema.inferSchema(starocksSchema, config);
    }

    @Override
    public Table getTable(StructType schema, Transform[] partitioning, Map<String, String> properties) {
        SimpleStarRocksConfig config = new SimpleStarRocksConfig(makeWriteCompatibleWithRead(properties));
        StarRocksSchema starRocksSchema = getStarRocksSchema(config);
        return new StarRocksTable(schema, starRocksSchema, config);
    }

    @Override
    public String shortName() {
        return "starrocks";
    }

    private StarRocksSchema getStarRocksSchema(StarRocksConfig config) {
        if (starocksSchema == null)  {
            starocksSchema = StarRocksConnector.getSchema(config);
        }

        return starocksSchema;
    }

    private static Map<String, String> makeWriteCompatibleWithRead(Map<String, String> options) {
        Map<String, String> compatibleOptions = new HashMap<>(options);

        // user and password compatible
        String user = options.get("user");
        if (user != null && !options.containsKey(STARROCKS_USER)) {
            compatibleOptions.put(STARROCKS_USER, user);
        }
        String password = options.get("password");
        if (password != null && !options.containsKey(STARROCKS_PASSWORD)) {
            compatibleOptions.put(STARROCKS_PASSWORD, password);
        }

        // FE http url compatible
        String feNodes = options.get(STARROCKS_FENODES);
        String feHttp = options.get(KEY_FE_HTTP);
        if (feNodes == null && feHttp != null) {
            compatibleOptions.put(STARROCKS_FENODES, feHttp);
        }
        if (feNodes != null && feHttp == null) {
            compatibleOptions.put(KEY_FE_HTTP, feNodes);
        }

        return compatibleOptions;
    }
}
