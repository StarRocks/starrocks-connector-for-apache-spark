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

import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.StructType;
import scala.collection.JavaConverters;

import java.io.Serializable;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

public abstract class AbstractRowStringConverter implements RowStringConverter, Serializable {

    private final Function<InternalRow, Row> internalRowConverter;
    private final DateTimeFormatter instantFormatter;
    protected final Function<Object, Object>[] valueConverters;

    @SuppressWarnings("unchecked")
    public AbstractRowStringConverter(StructType schema, ZoneId timeZone) {
        this.internalRowConverter = new InternalRowToRowFunction(schema);
        this.instantFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss[.SSSSSS]").withZone(timeZone);
        this.valueConverters = (Function<Object, Object>[]) new Function[schema.length()];
        for (int i = 0; i < schema.size(); i++) {
            valueConverters[i] = convert(schema.fields()[i].dataType());
        }
    }

    @Override
    public String fromRow(InternalRow row) {
        return fromRow(internalRowConverter.apply(row));
    }

    protected Function<Object, Object> convert(DataType dataType) {
        try {
            if (DataTypes.StringType.acceptsType(dataType)
                    || DataTypes.BooleanType.acceptsType(dataType)
                    || DataTypes.DoubleType.acceptsType(dataType)
                    || DataTypes.FloatType.acceptsType(dataType)
                    || DataTypes.ByteType.acceptsType(dataType)
                    || DataTypes.IntegerType.acceptsType(dataType)
                    || DataTypes.LongType.acceptsType(dataType)
                    || DataTypes.ShortType.acceptsType(dataType)) {
                return arg -> arg;
            } else if (DataTypes.DateType.acceptsType(dataType)) {
                // if spark.sql.datetime.java8API.enabled is false, data will be a java.sql.Date,
                // otherwise a java.time.LocalDate. The toString methods for both class will return
                // the date in the same format, that's  uuuu-MM-dd or yyyy-mm-dd, such as 2013-07-15
                return new NullableWrapper(Object::toString);
            } else if (DataTypes.TimestampType.acceptsType(dataType)) {
                // if spark.sql.datetime.java8API.enabled is false, data will be a java.sql.Timestamp,
                // otherwise a java.time.Instant
                return new NullableWrapper(
                        arg -> {
                            if (arg instanceof Timestamp) {
                                return (instantFormatter.format(((Timestamp) arg).toInstant()));
                            } else {
                                return (instantFormatter.format((Instant) arg));
                            }
                        }
                );
            } else if (dataType instanceof DecimalType) {
                return new NullableWrapper(
                        arg -> arg instanceof BigDecimal
                                ? arg
                                : ((Decimal) arg).toBigDecimal().bigDecimal()
                );
            } else if (dataType instanceof ArrayType) {
                DataType elementType = ((ArrayType) dataType).elementType();
                return new NullableWrapper(new ListDataConverter(convert(elementType)));
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        throw new RuntimeException(String.format("Invalid type %s", dataType));
    }

    private static class NullableWrapper implements Function<Object, Object> {

        private final Function<Object, Object> function;

        public NullableWrapper(Function<Object, Object> function) {
            this.function = function;
        }

        @Override
        public Object apply(Object data) {
            return data == null ? null : function.apply(data);
        }
    }

    private static class ListDataConverter implements Function<Object, Object> {

        private final Function<Object, Object> elementConverter;

        public ListDataConverter(Function<Object, Object> elementConverter) {
            this.elementConverter = elementConverter;
        }

        @SuppressWarnings("unchecked")
        @Override
        public Object apply(Object data) {
            List<?> input;
            // array type is scala.collection.Seq or java.util.List
            if (data instanceof List) {
                input = (List<?>) data;
            } else {
                input = JavaConverters.seqAsJavaList((scala.collection.Seq<Object>) data);
            }
            List<Object> result = new ArrayList<>(input.size());
            input.forEach(element -> result.add(elementConverter.apply(element)));
            return result;
        }
    }
}
