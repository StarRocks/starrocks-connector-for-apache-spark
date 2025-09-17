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

import com.starrocks.connector.spark.exception.StarRocksException;
import com.starrocks.connector.spark.sql.conf.SimpleStarRocksConfig;
import com.starrocks.connector.spark.sql.conf.StarRocksConfig;
import com.starrocks.connector.spark.sql.connect.StarRocksConnector;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public final class InferSchema {

    public static StructType inferSchema(Map<String, String> options) {
        SimpleStarRocksConfig config = new SimpleStarRocksConfig(options);
        StarRocksSchema starrocksSchema = StarRocksConnector.getSchema(config, null);
        return inferSchema(starrocksSchema, config);
    }

    public static StructType inferSchema(StarRocksSchema starRocksSchema, StarRocksConfig config) {
        String[] inputColumns = config.getColumns();
        List<StarRocksField> starRocksFields;
        if (inputColumns == null || inputColumns.length == 0) {
            starRocksFields = starRocksSchema.getColumns();
        } else {
            starRocksFields = new ArrayList<>();
            List<String> nonExistedColumns = new ArrayList<>();
            for (String column : inputColumns) {
                StarRocksField field = starRocksSchema.getField(column);
                if (field == null) {
                    nonExistedColumns.add(column);
                }
                starRocksFields.add(field);
            }
            if (!nonExistedColumns.isEmpty()) {
                throw new StarRocksException(String.format("Can't find those columns %s in StarRocks table `%s`.`%s`. "
                                + "Please check your configuration 'starrocks.columns' to make sure all columns exist in the table",
                        nonExistedColumns, config.getDatabase(), config.getTable()));
            }
        }

        Map<String, StructField> customTypes = parseCustomTypes(config.getColumnTypes());
        List<StructField> fields = new ArrayList<>();
        for (StarRocksField field : starRocksFields) {
            if (customTypes.containsKey(field.getName())) {
                fields.add(customTypes.get(field.getName()));
            } else {
                fields.add(inferStructField(field));
            }
        }

        return DataTypes.createStructType(fields);
    }

    static Map<String, StructField> parseCustomTypes(String columnTypes) {
        if (columnTypes == null) {
            return new HashMap<>();
        }

        Map<String, StructField> customTypes = new HashMap<>();
        StructType customSchema = StructType.fromDDL(columnTypes);
        for (StructField field : customSchema.fields()) {
            customTypes.put(field.name(), field);
        }
        return customTypes;
    }

    static StructField inferStructField(StarRocksField field) {
        DataType dataType = inferDataType(field);

        return new StructField(field.getName(), dataType, true, Metadata.empty());
    }

    static DataType inferDataType(StarRocksField field) {
        String type = field.getType().toLowerCase(Locale.ROOT);
        switch (type) {
            case "tinyint":
                // mysql does not have boolean type, and starrocks `information_schema`.`COLUMNS` will return
                // a "tinyint" data type for both StarRocks BOOLEAN and TINYINT type, We distinguish them by
                // column size, and the size of BOOLEAN is null
                return field.getSize() == null ? DataTypes.BooleanType : DataTypes.ByteType;
            case "smallint":
                return DataTypes.ShortType;
            case "int":
                return DataTypes.IntegerType;
            case "bigint":
                return DataTypes.LongType;
            case "bigint unsigned":
                return DataTypes.StringType;
            case "largeint":
                return DataTypes.StringType;
            case "float":
                return DataTypes.FloatType;
            case "double":
                return DataTypes.DoubleType;
            case "decimal":
            case "decimalv2":
            case "decimal32":
            case "decimal64":
            case "decimal128":
                return DataTypes.createDecimalType(field.getPrecision(), field.getScale());
            case "char":
            case "varchar":
            case "string":
            case "json":
                return DataTypes.StringType;
            case "date":
                return DataTypes.DateType;
            case "datetime":
                return DataTypes.TimestampType;
            case "boolean":
                return DataTypes.BooleanType;
            default:
                throw new UnsupportedOperationException(
                        String.format("Unsupported starrocks type, column name: %s, data type: %s", field.getName(),
                                field.getType()));
        }
    }
}
