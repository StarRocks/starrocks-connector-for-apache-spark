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
import com.starrocks.connector.spark.exception.StarRocksException;
import com.starrocks.connector.spark.rest.models.FieldType;
import com.starrocks.connector.spark.sql.schema.StarRocksField;
import com.starrocks.connector.spark.sql.schema.StarRocksSchema;
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
import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Optional;

/**
 * row batch data container.
 */
public class RpcRowBatch extends BaseRowBatch {
    private static Logger logger = LoggerFactory.getLogger(RpcRowBatch.class);

    private final ArrowStreamReader arrowStreamReader;
    private final VectorSchemaRoot root;
    private List<FieldVector> fieldVectors;
    private RootAllocator rootAllocator;

    public RpcRowBatch(TScanBatchResult nextResult, StarRocksSchema schema) throws StarRocksException {
        this(nextResult, schema, ZoneId.systemDefault());
    }

    public RpcRowBatch(TScanBatchResult nextResult, StarRocksSchema schema, ZoneId srTimeZoneId)
            throws StarRocksException {
        super(schema, srTimeZoneId);
        this.rootAllocator = new RootAllocator(Integer.MAX_VALUE);
        this.arrowStreamReader = new ArrowStreamReader(new ByteArrayInputStream(nextResult.getRows()), rootAllocator);
        try {
            this.root = arrowStreamReader.getVectorSchemaRoot();
            while (arrowStreamReader.loadNextBatch()) {
                fieldVectors = root.getFieldVectors();
                if (fieldVectors.size() != schema.getColumns().size()) {
                    logger.error("Schema size '{}' is not equal to arrow field size '{}'.",
                            schema.getColumns().size(), fieldVectors.size());
                    throw new StarRocksException("Load StarRocks data failed, schema size of fetch data is wrong.");
                }
                if (fieldVectors.isEmpty() || root.getRowCount() == 0) {
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
            throw new StarRocksException(e);
        } finally {
            close();
        }
    }

    public void convertArrowToRowBatch() {
        try {
            for (int col = 0; col < fieldVectors.size(); col++) {
                FieldVector curFieldVector = fieldVectors.get(col);
                Types.MinorType mt = curFieldVector.getMinorType();
                String currentType = resolveFieldType(col);

                switch (FieldType.of(currentType)) {
                    case NULL:
                        for (int rowIndex = 0; rowIndex < rowCountInOneBatch; rowIndex++) {
                            addValueToRow(rowIndex, null);
                        }
                        break;
                    case BOOLEAN:
                        Preconditions.checkArgument(mt.equals(Types.MinorType.BIT),
                                typeMismatchMessage(currentType, mt));
                        BitVector bitVector = (BitVector) curFieldVector;
                        for (int rowIndex = 0; rowIndex < rowCountInOneBatch; rowIndex++) {
                            Object fieldValue = bitVector.isNull(rowIndex) ? null : bitVector.get(rowIndex) != 0;
                            addValueToRow(rowIndex, fieldValue);
                        }
                        break;
                    case TINYINT:
                        Preconditions.checkArgument(mt.equals(Types.MinorType.TINYINT),
                                typeMismatchMessage(currentType, mt));
                        TinyIntVector tinyIntVector = (TinyIntVector) curFieldVector;
                        for (int rowIndex = 0; rowIndex < rowCountInOneBatch; rowIndex++) {
                            Object fieldValue = tinyIntVector.isNull(rowIndex) ? null : tinyIntVector.get(rowIndex);
                            addValueToRow(rowIndex, fieldValue);
                        }
                        break;
                    case SMALLINT:
                        Preconditions.checkArgument(mt.equals(Types.MinorType.SMALLINT),
                                typeMismatchMessage(currentType, mt));
                        SmallIntVector smallIntVector = (SmallIntVector) curFieldVector;
                        for (int rowIndex = 0; rowIndex < rowCountInOneBatch; rowIndex++) {
                            Object fieldValue = smallIntVector.isNull(rowIndex) ? null : smallIntVector.get(rowIndex);
                            addValueToRow(rowIndex, fieldValue);
                        }
                        break;
                    case INT:
                        Preconditions.checkArgument(mt.equals(Types.MinorType.INT),
                                typeMismatchMessage(currentType, mt));
                        IntVector intVector = (IntVector) curFieldVector;
                        for (int rowIndex = 0; rowIndex < rowCountInOneBatch; rowIndex++) {
                            Object fieldValue = intVector.isNull(rowIndex) ? null : intVector.get(rowIndex);
                            addValueToRow(rowIndex, fieldValue);
                        }
                        break;
                    case BIGINT:
                        Preconditions.checkArgument(mt.equals(Types.MinorType.BIGINT),
                                typeMismatchMessage(currentType, mt));
                        BigIntVector bigIntVector = (BigIntVector) curFieldVector;
                        for (int rowIndex = 0; rowIndex < rowCountInOneBatch; rowIndex++) {
                            Object fieldValue = bigIntVector.isNull(rowIndex) ? null : bigIntVector.get(rowIndex);
                            addValueToRow(rowIndex, fieldValue);
                        }
                        break;
                    case FLOAT:
                        Preconditions.checkArgument(mt.equals(Types.MinorType.FLOAT4),
                                typeMismatchMessage(currentType, mt));
                        Float4Vector float4Vector = (Float4Vector) curFieldVector;
                        for (int rowIndex = 0; rowIndex < rowCountInOneBatch; rowIndex++) {
                            Object fieldValue = float4Vector.isNull(rowIndex) ? null : float4Vector.get(rowIndex);
                            addValueToRow(rowIndex, fieldValue);
                        }
                        break;
                    case TIME:
                    case DOUBLE:
                        Preconditions.checkArgument(mt.equals(Types.MinorType.FLOAT8),
                                typeMismatchMessage(currentType, mt));
                        Float8Vector float8Vector = (Float8Vector) curFieldVector;
                        for (int rowIndex = 0; rowIndex < rowCountInOneBatch; rowIndex++) {
                            Object fieldValue = float8Vector.isNull(rowIndex) ? null : float8Vector.get(rowIndex);
                            addValueToRow(rowIndex, fieldValue);
                        }
                        break;
                    case BINARY:
                        Preconditions.checkArgument(mt.equals(Types.MinorType.VARBINARY),
                                typeMismatchMessage(currentType, mt));
                        VarBinaryVector varBinaryVector = (VarBinaryVector) curFieldVector;
                        for (int rowIndex = 0; rowIndex < rowCountInOneBatch; rowIndex++) {
                            Object fieldValue = varBinaryVector.isNull(rowIndex) ? null : varBinaryVector.get(rowIndex);
                            addValueToRow(rowIndex, fieldValue);
                        }
                        break;
                    case DECIMAL:
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
                                throw new StarRocksException(errMsg);
                            }
                            addValueToRow(rowIndex, decimal);
                        }
                        break;
                    case DECIMALV2:
                    case DECIMAL32:
                    case DECIMAL64:
                    case DECIMAL128:
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
                    case DATE:
                        Preconditions.checkArgument(mt.equals(Types.MinorType.VARCHAR),
                                typeMismatchMessage(currentType, mt));
                        VarCharVector varCharVectorForDate = (VarCharVector) curFieldVector;
                        for (int rowIndex = 0; rowIndex < rowCountInOneBatch; rowIndex++) {
                            if (varCharVectorForDate.isNull(rowIndex)) {
                                addValueToRow(rowIndex, null);
                                continue;
                            }
                            String value = new String(varCharVectorForDate.get(rowIndex));
                            LocalDate parsedTime = LocalDate.parse(value, dateFormatter);

                            addValueToRow(rowIndex, Date.valueOf(parsedTime));
                        }
                        break;
                    case DATETIME:
                        Preconditions.checkArgument(mt.equals(Types.MinorType.VARCHAR),
                                typeMismatchMessage(currentType, mt));
                        VarCharVector varCharVectorForDateTime = (VarCharVector) curFieldVector;
                        for (int rowIndex = 0; rowIndex < rowCountInOneBatch; rowIndex++) {
                            if (varCharVectorForDateTime.isNull(rowIndex)) {
                                addValueToRow(rowIndex, null);
                                continue;
                            }
                            String value = new String(varCharVectorForDateTime.get(rowIndex));
                            ZonedDateTime zonedDateTime = ZonedDateTime.parse(value, dateTimeFormatter);
                            addValueToRow(rowIndex, Timestamp.from(zonedDateTime.toInstant()));
                        }
                        break;
                    case LARGEINT:
                    case VARCHAR:
                    case CHAR:
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
                        String errMsg = "Unsupported type " + schema.getColumns().get(col).getType();
                        logger.error(errMsg);
                        throw new StarRocksException(errMsg);
                }
            }
        } catch (Exception e) {
            close();
            throw e;
        }
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

    private String resolveFieldType(int colIdx) {
        FieldVector curFieldVector = fieldVectors.get(colIdx);

        StarRocksField vectorField = fieldMap.get(curFieldVector.getName());
        if (null != vectorField && FieldType.isSupported(vectorField.getType())) {
            return vectorField.getType();
        }

        StarRocksField schemaField = schema.getColumns().get(colIdx);
        if (null != schemaField && FieldType.isSupported(schemaField.getType())) {
            logger.warn("Resolve field type[{}] from schema, idx: {}, vector[name: {}, type: {}]",
                    schemaField.getType(), colIdx, curFieldVector.getName(),
                    Optional.ofNullable(vectorField).map(StarRocksField::getType).orElse(null));
            return schemaField.getType();
        }

        return DataTypeUtils.map(curFieldVector.getMinorType());
    }

}
