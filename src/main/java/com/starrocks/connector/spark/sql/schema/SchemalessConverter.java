/*
 * Copyright 2021-present StarRocks, Inc. All rights reserved.
 *
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package com.starrocks.connector.spark.sql.schema;

import com.starrocks.connector.spark.exception.StarrocksException;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.time.ZoneId;

public class SchemalessConverter extends AbstractRowStringConverter {

    public final static StructType SCHEMA = StructType.fromDDL("c0 STRING");

    public SchemalessConverter(ZoneId timeZone) {
        super(SCHEMA, timeZone);
    }

    @Override
    public String fromRow(Row row) {
        checkSchema(row.schema());
        return valueConverters[0].apply(row.get(0)).toString();
    }

    private void checkSchema(StructType schema) {
        if (schema == null) {
            throw new StarrocksException("Can't convert row without schema");
        }

        if (schema.length() > 1 || !schema.apply(0).dataType().sameType(DataTypes.StringType)) {
            throw new StarrocksException("Schemaless converter expects a schema with only one STRING column, " +
                    "but the actual schema is " + schema);
        }
    }
}
