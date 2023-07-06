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
import org.apache.spark.sql.catalyst.encoders.RowEncoder$;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.types.StructType;
import scala.collection.Seq;

import java.io.Serializable;
import java.util.function.Function;

/**
 * Refer to mongo-spark
 * https://github.com/mongodb/mongo-spark/blob/main/src/main/java/com/mongodb/spark/sql/connector/schema/InternalRowToRowFunction.java.
 */
public class InternalRowToRowFunction implements Function<InternalRow, Row>, Serializable {

    private static final long serialVersionUID = 1L;

    private final ExpressionEncoder.Deserializer<Row> deserializer;

    public InternalRowToRowFunction(StructType schema) {
        ExpressionEncoder<Row> rowExpressionEncoder = RowEncoder$.MODULE$.apply(schema);

        Seq<Attribute> attributeSeq = (Seq<Attribute>) (Seq<? extends Attribute>)
                rowExpressionEncoder.schema().toAttributes();

        this.deserializer = rowExpressionEncoder.resolveAndBind(attributeSeq, SimpleAnalyzer$.MODULE$).createDeserializer();
    }

    @Override
    public Row apply(InternalRow internalRow) {
        return deserializer.apply(internalRow);
    }
}
