// Modifications Copyright 2021 StarRocks Limited.
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

package com.starrocks.connector.spark.serialization;

import com.google.common.base.Preconditions;
import com.starrocks.connector.spark.exception.StarrocksException;
import com.starrocks.connector.spark.rest.models.Field;
import com.starrocks.connector.spark.rest.models.Schema;
import com.starrocks.connector.spark.util.DataTypeUtils;
import com.starrocks.thrift.TScanBatchResult;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.types.Types;
import org.apache.spark.sql.types.Decimal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * row batch data container.
 */
public class RowBatch {
    private static Logger logger = LoggerFactory.getLogger(RowBatch.class);

    public static class Row {
        private List<Object> cols;

        Row(int colCount) {
            this.cols = new ArrayList<>(colCount);
        }

        List<Object> getCols() {
            return cols;
        }

        public void put(Object o) {
            cols.add(o);
        }
    }

    // offset for iterate the rowBatch
    private int offsetInRowBatch = 0;
    private int rowCountInOneBatch = 0;
    private int readRowCount = 0;
    private List<Row> rowBatch = new ArrayList<>();
    private final ArrowStreamReader arrowStreamReader;
    private final VectorSchemaRoot root;
    private List<FieldVector> fieldVectors;
    private RootAllocator rootAllocator;
    private final Schema schema;
    private final Map<String, Field> fieldMap;

    public RowBatch(TScanBatchResult nextResult, Schema schema) throws StarrocksException {
        this.schema = schema;
        this.fieldMap = schema.getProperties().stream()
                .collect(
                        Collectors.toMap(
                                Field::getName,
                                Function.identity()
                        )
                );
        this.rootAllocator = new RootAllocator(Integer.MAX_VALUE);
        this.arrowStreamReader = new ArrowStreamReader(
                new ByteArrayInputStream(nextResult.getRows()),
                rootAllocator
        );
        this.offsetInRowBatch = 0;
        try {
            this.root = arrowStreamReader.getVectorSchemaRoot();
            while (arrowStreamReader.loadNextBatch()) {
                fieldVectors = root.getFieldVectors();
                if (fieldVectors.size() != schema.size()) {
                    logger.error("Schema size '{}' is not equal to arrow field size '{}'.",
                            schema.size(), fieldVectors.size());
                    throw new StarrocksException("Load StarRocks data failed, schema size of fetch data is wrong.");
                }
                if (fieldVectors.size() == 0 || root.getRowCount() == 0) {
                    logger.debug("One batch in arrow has no data.");
                    continue;
                }
                rowCountInOneBatch = root.getRowCount();
                // init the rowBatch
                for (int i = 0; i < rowCountInOneBatch; ++i) {
                    rowBatch.add(new Row(fieldVectors.size()));
                }
                convertArrowToRowBatch();
                readRowCount += root.getRowCount();
            }
        } catch (Exception e) {
            logger.error("Read StarRocks Data failed because: ", e);
            throw new StarrocksException(e.getMessage());
        } finally {
            close();
        }
    }

    public boolean hasNext() {
        if (offsetInRowBatch < readRowCount) {
            return true;
        }
        return false;
    }

    private void addValueToRow(int rowIndex, Object obj) {
        if (rowIndex > rowCountInOneBatch) {
            String errMsg = "Get row offset: " + rowIndex + " larger than row size: " +
                    rowCountInOneBatch;
            logger.error(errMsg);
            throw new NoSuchElementException(errMsg);
        }
        rowBatch.get(readRowCount + rowIndex).put(obj);
    }

    public void convertArrowToRowBatch() throws StarrocksException {
        try {
            for (int col = 0; col < fieldVectors.size(); col++) {
                FieldVector curFieldVector = fieldVectors.get(col);
                Types.MinorType mt = curFieldVector.getMinorType();
                Field field = fieldMap.get(curFieldVector.getName());

                String currentType;

                if (field != null) {
                    currentType = field.getType();
                } else {
                    currentType = DataTypeUtils.map(mt);
                }

                switch (currentType) {
                    case "NULL_TYPE":
                        for (int rowIndex = 0; rowIndex < rowCountInOneBatch; rowIndex++) {
                            addValueToRow(rowIndex, null);
                        }
                        break;
                    case "BOOLEAN":
                        Preconditions.checkArgument(mt.equals(Types.MinorType.BIT),
                                typeMismatchMessage(currentType, mt));
                        BitVector bitVector = (BitVector) curFieldVector;
                        for (int rowIndex = 0; rowIndex < rowCountInOneBatch; rowIndex++) {
                            Object fieldValue = bitVector.isNull(rowIndex) ? null : bitVector.get(rowIndex) != 0;
                            addValueToRow(rowIndex, fieldValue);
                        }
                        break;
                    case "TINYINT":
                        Preconditions.checkArgument(mt.equals(Types.MinorType.TINYINT),
                                typeMismatchMessage(currentType, mt));
                        TinyIntVector tinyIntVector = (TinyIntVector) curFieldVector;
                        for (int rowIndex = 0; rowIndex < rowCountInOneBatch; rowIndex++) {
                            Object fieldValue = tinyIntVector.isNull(rowIndex) ? null : tinyIntVector.get(rowIndex);
                            addValueToRow(rowIndex, fieldValue);
                        }
                        break;
                    case "SMALLINT":
                        Preconditions.checkArgument(mt.equals(Types.MinorType.SMALLINT),
                                typeMismatchMessage(currentType, mt));
                        SmallIntVector smallIntVector = (SmallIntVector) curFieldVector;
                        for (int rowIndex = 0; rowIndex < rowCountInOneBatch; rowIndex++) {
                            Object fieldValue = smallIntVector.isNull(rowIndex) ? null : smallIntVector.get(rowIndex);
                            addValueToRow(rowIndex, fieldValue);
                        }
                        break;
                    case "INT":
                        Preconditions.checkArgument(mt.equals(Types.MinorType.INT),
                                typeMismatchMessage(currentType, mt));
                        IntVector intVector = (IntVector) curFieldVector;
                        for (int rowIndex = 0; rowIndex < rowCountInOneBatch; rowIndex++) {
                            Object fieldValue = intVector.isNull(rowIndex) ? null : intVector.get(rowIndex);
                            addValueToRow(rowIndex, fieldValue);
                        }
                        break;
                    case "BIGINT":
                        Preconditions.checkArgument(mt.equals(Types.MinorType.BIGINT),
                                typeMismatchMessage(currentType, mt));
                        BigIntVector bigIntVector = (BigIntVector) curFieldVector;
                        for (int rowIndex = 0; rowIndex < rowCountInOneBatch; rowIndex++) {
                            Object fieldValue = bigIntVector.isNull(rowIndex) ? null : bigIntVector.get(rowIndex);
                            addValueToRow(rowIndex, fieldValue);
                        }
                        break;
                    case "FLOAT":
                        Preconditions.checkArgument(mt.equals(Types.MinorType.FLOAT4),
                                typeMismatchMessage(currentType, mt));
                        Float4Vector float4Vector = (Float4Vector) curFieldVector;
                        for (int rowIndex = 0; rowIndex < rowCountInOneBatch; rowIndex++) {
                            Object fieldValue = float4Vector.isNull(rowIndex) ? null : float4Vector.get(rowIndex);
                            addValueToRow(rowIndex, fieldValue);
                        }
                        break;
                    case "TIME":
                    case "DOUBLE":
                        Preconditions.checkArgument(mt.equals(Types.MinorType.FLOAT8),
                                typeMismatchMessage(currentType, mt));
                        Float8Vector float8Vector = (Float8Vector) curFieldVector;
                        for (int rowIndex = 0; rowIndex < rowCountInOneBatch; rowIndex++) {
                            Object fieldValue = float8Vector.isNull(rowIndex) ? null : float8Vector.get(rowIndex);
                            addValueToRow(rowIndex, fieldValue);
                        }
                        break;
                    case "BINARY":
                        Preconditions.checkArgument(mt.equals(Types.MinorType.VARBINARY),
                                typeMismatchMessage(currentType, mt));
                        VarBinaryVector varBinaryVector = (VarBinaryVector) curFieldVector;
                        for (int rowIndex = 0; rowIndex < rowCountInOneBatch; rowIndex++) {
                            Object fieldValue = varBinaryVector.isNull(rowIndex) ? null : varBinaryVector.get(rowIndex);
                            addValueToRow(rowIndex, fieldValue);
                        }
                        break;
                    case "DECIMAL":
                        Preconditions.checkArgument(mt.equals(Types.MinorType.VARCHAR),
                                typeMismatchMessage(currentType, mt));
                        VarCharVector varCharVectorForDecimal = (VarCharVector) curFieldVector;
                        for (int rowIndex = 0; rowIndex < rowCountInOneBatch; rowIndex++) {
                            if (varCharVectorForDecimal.isNull(rowIndex)) {
                                addValueToRow(rowIndex, null);
                                continue;
                            }
                            String decimalValue = new String(varCharVectorForDecimal.get(rowIndex));
                            Decimal decimal = new Decimal();
                            try {
                                decimal.set(new scala.math.BigDecimal(new BigDecimal(decimalValue)));
                            } catch (NumberFormatException e) {
                                String errMsg = "Decimal response result '" + decimalValue + "' is illegal.";
                                logger.error(errMsg, e);
                                throw new StarrocksException(errMsg);
                            }
                            addValueToRow(rowIndex, decimal);
                        }
                        break;
                    case "DECIMALV2":
                    case "DECIMAL32":
                    case "DECIMAL64":
                    case "DECIMAL128":
                        Preconditions.checkArgument(mt.equals(Types.MinorType.DECIMAL),
                                typeMismatchMessage(currentType, mt));
                        DecimalVector decimalVector = (DecimalVector) curFieldVector;
                        for (int rowIndex = 0; rowIndex < rowCountInOneBatch; rowIndex++) {
                            if (decimalVector.isNull(rowIndex)) {
                                addValueToRow(rowIndex, null);
                                continue;
                            }
                            Decimal decimal = Decimal.apply(decimalVector.getObject(rowIndex));
                            addValueToRow(rowIndex, decimal);
                        }
                        break;
                    case "DATE":
                    case "DATETIME":
                    case "LARGEINT":
                    case "CHAR":
                    case "VARCHAR":
                        Preconditions.checkArgument(mt.equals(Types.MinorType.VARCHAR),
                                typeMismatchMessage(currentType, mt));
                        VarCharVector varCharVector = (VarCharVector) curFieldVector;
                        for (int rowIndex = 0; rowIndex < rowCountInOneBatch; rowIndex++) {
                            if (varCharVector.isNull(rowIndex)) {
                                addValueToRow(rowIndex, null);
                                continue;
                            }
                            String value = new String(varCharVector.get(rowIndex));
                            addValueToRow(rowIndex, value);
                        }
                        break;
                    default:
                        String errMsg = "Unsupported type " + schema.get(col).getType();
                        logger.error(errMsg);
                        throw new StarrocksException(errMsg);
                }
            }
        } catch (Exception e) {
            close();
            throw e;
        }
    }

    public List<Object> next() throws StarrocksException {
        if (!hasNext()) {
            String errMsg = "Get row offset:" + offsetInRowBatch + " larger than row size: " + readRowCount;
            logger.error(errMsg);
            throw new NoSuchElementException(errMsg);
        }
        return rowBatch.get(offsetInRowBatch++).getCols();
    }

    private String typeMismatchMessage(final String sparkType, final Types.MinorType arrowType) {
        final String messageTemplate = "Spark type is %1$s, but arrow type is %2$s.";
        return String.format(messageTemplate, sparkType, arrowType.name());
    }

    public int getReadRowCount() {
        return readRowCount;
    }

    public void close() {
        try {
            if (arrowStreamReader != null) {
                arrowStreamReader.close();
            }
            if (rootAllocator != null) {
                rootAllocator.close();
            }
        } catch (IOException ioe) {
            // do nothing
        }
    }
}
