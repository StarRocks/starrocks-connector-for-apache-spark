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

import com.starrocks.connector.spark.sql.conf.SimpleStarRocksConfig;
import com.starrocks.connector.spark.sql.conf.StarRocksConfigBase;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class InferSchemaTest {

    @Test
    public void testStructOverrideApplied() {
        List<StarRocksField> fields = Arrays.asList(
                new StarRocksField("purpose_id", "varchar", 0, null, null, null),
                new StarRocksField("identifier", "struct", 1, null, null, null)
        );
        StarRocksSchema schema = new StarRocksSchema(fields);

        Map<String, String> options = new HashMap<>();
        options.put(StarRocksConfigBase.KEY_COLUMN_TYPES,
                "`identifier` STRUCT<`type`: STRING, `id`: STRING>");
        SimpleStarRocksConfig config = new SimpleStarRocksConfig(options);

        StructType result = InferSchema.inferSchema(schema, config);
        StructField identifier = result.apply("identifier");
        Assert.assertTrue(identifier.dataType() instanceof StructType);
        StructType identifierType = (StructType) identifier.dataType();
        Assert.assertEquals(DataTypes.StringType, identifierType.apply("type").dataType());
        Assert.assertEquals(DataTypes.StringType, identifierType.apply("id").dataType());
    }
}

