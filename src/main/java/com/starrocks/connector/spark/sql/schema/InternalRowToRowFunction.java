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
import org.apache.spark.sql.catalyst.analysis.SimpleAnalyzer$;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder$;
import org.apache.spark.sql.catalyst.encoders.RowEncoder$;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.expressions.AttributeReference;
import org.apache.spark.sql.catalyst.expressions.NamedExpression;
import org.apache.spark.sql.types.StructType;
import scala.collection.JavaConverters;
import scala.collection.Seq;
import scala.collection.Seq$;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Refer to mongo-spark
 * https://github.com/mongodb/mongo-spark/blob/main/src/main/java/com/mongodb/spark/sql/connector/schema
 * /InternalRowToRowFunction.java.
 */
public class InternalRowToRowFunction implements Function<InternalRow, Row>, Serializable {

    private static final long serialVersionUID = 1L;

    private final ExpressionEncoder.Deserializer<Row> deserializer;

    public InternalRowToRowFunction(StructType schema) {
        ExpressionEncoder<Row> rowExpressionEncoder;
        Object instance;
        Method applyMethod;

        // before 3.5, we use RowEncoder.apply to create ExpressionEncoder
        // 3.5, we use ExpressionEncoder.apply to create it.
        try {
            applyMethod = RowEncoder$.MODULE$.getClass().getMethod("apply", StructType.class);
            instance = RowEncoder$.MODULE$;
        } catch (NoSuchMethodException e1) {
            try {
                applyMethod = ExpressionEncoder$.MODULE$.getClass().getMethod("apply", StructType.class);
                instance = ExpressionEncoder$.MODULE$;
            } catch (NoSuchMethodException e2) {
                throw new RuntimeException("No method to create InternalRowToRowFunction");
            }
        }

        try {
            rowExpressionEncoder = (ExpressionEncoder<Row>) applyMethod.invoke(instance, schema);
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException("Fail to call applyMethod to create InternalRowToRowFunction");
        }

        List<Attribute> attributeList = (List<Attribute>) Arrays.stream(rowExpressionEncoder.schema().fields())
                .map(x -> (Attribute) new AttributeReference(x.name(), x.dataType(), x.nullable(), x.metadata(),
                        NamedExpression.newExprId(), Seq$.MODULE$.empty())).collect(Collectors.toList());
        Seq<Attribute> attributeSeq = JavaConverters.collectionAsScalaIterable(attributeList).toSeq();
        this.deserializer = rowExpressionEncoder.resolveAndBind(attributeSeq, SimpleAnalyzer$.MODULE$).createDeserializer();

    }

    @Override
    public Row apply(InternalRow internalRow) {
        return deserializer.apply(internalRow);
    }
}
