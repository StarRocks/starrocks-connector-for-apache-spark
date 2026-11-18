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
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
        assertTrue(identifier.dataType() instanceof StructType);
        StructType identifierType = (StructType) identifier.dataType();
        assertEquals(DataTypes.StringType, identifierType.apply("type").dataType());
        assertEquals(DataTypes.StringType, identifierType.apply("id").dataType());
    }

    @Test
    public void testInferArrayTypeFromColumnType() {
        assertEquals(
                DataTypes.createArrayType(DataTypes.LongType, true),
                inferDataType("array<bigint(20)>"));
        assertEquals(
                DataTypes.createArrayType(DataTypes.createArrayType(DataTypes.IntegerType, true), true),
                inferDataType("array<array<int(11)>>"));
        assertEquals(
                DataTypes.createArrayType(DataTypes.BooleanType, true),
                inferDataType("array<boolean>"));
        assertEquals(
                DataTypes.createArrayType(DataTypes.ByteType, true),
                inferDataType("array<tinyint(4)>"));
    }

    @Test
    public void testInferMapTypeFromColumnType() {
        MapType expected = DataTypes.createMapType(
                DataTypes.StringType,
                DataTypes.createArrayType(new DecimalType(10, 2), true),
                true);
        assertEquals(expected, inferDataType("map<varchar(65533),array<DECIMAL64(10,2)>>"));
    }

    @Test
    public void testInferStructTypeFromColumnType() {
        StructType expected = DataTypes.createStructType(Arrays.asList(
                DataTypes.createStructField("a", DataTypes.IntegerType, true),
                DataTypes.createStructField(
                        "b", DataTypes.createArrayType(DataTypes.TimestampType, true), true),
                DataTypes.createStructField("order id", DataTypes.LongType, true)
        ));
        assertEquals(
                expected,
                inferDataType(
                        "struct<a int(11), b array<datetime>, `order id` bigint(20)>"));
    }

    @Test
    public void testInferNestedComplexTypeFromColumnType() {
        StructType valueType = DataTypes.createStructType(Arrays.asList(
                DataTypes.createStructField("flag", DataTypes.BooleanType, true),
                DataTypes.createStructField("amount", new DecimalType(20, 3), true)
        ));
        MapType mapType = DataTypes.createMapType(DataTypes.StringType, valueType, true);
        assertEquals(
                DataTypes.createArrayType(mapType, true),
                inferDataType("array<map<string,struct<flag boolean,amount decimal128(20,3)>>>"));
    }

    @Test
    public void testIncompleteOrInvalidComplexTypeIsUnsupported() {
        assertThrows(UnsupportedOperationException.class, () -> inferDataType("array"));
        assertThrows(UnsupportedOperationException.class, () -> inferDataType("array<bigint(20)"));
        assertThrows(UnsupportedOperationException.class, () -> inferDataType("map<string>"));
        assertThrows(UnsupportedOperationException.class, () -> inferDataType("array<int(11) unsigned>"));
    }

    private static org.apache.spark.sql.types.DataType inferDataType(String type) {
        return InferSchema.inferDataType(new StarRocksField("c1", type, 0, null, null, null));
    }
}
