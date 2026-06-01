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
import org.apache.arrow.vector.complex.StructVector;
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
import org.apache.spark.unsafe.types.UTF8String;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;
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
                FieldType fieldType = FieldType.of(currentType);
                StarRocksField schemaField = schema.getColumns().get(col);

                // For complex types (STRUCT/ARRAY/MAP), use the generic convertValue path
                if (fieldType == FieldType.STRUCT || fieldType == FieldType.ARRAY || fieldType == FieldType.MAP) {
                    for (int rowIndex = 0; rowIndex < rowCountInOneBatch; rowIndex++) {
                        Object value = convertValue(curFieldVector, mt, fieldType, rowIndex, schemaField);
                        addValueToRow(rowIndex, value);
                    }
                    continue;
                }

                // For primitive types, use the original tight loop (cast vector once, iterate rows)
                switch (fieldType) {
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

    private Object convertValue(FieldVector vector,
                                Types.MinorType arrowType,
                                FieldType fieldType,
                                int rowIndex,
                                StarRocksField schemaField) {
        String sparkType = fieldType.getTypeName();
        switch (fieldType) {
            case NULL:
                return null;
            case BOOLEAN:
                Preconditions.checkArgument(arrowType.equals(Types.MinorType.BIT),
                        typeMismatchMessage(sparkType, arrowType));
                BitVector bitVector = (BitVector) vector;
                return bitVector.isNull(rowIndex) ? null : bitVector.get(rowIndex) != 0;
            case TINYINT:
                Preconditions.checkArgument(arrowType.equals(Types.MinorType.TINYINT),
                        typeMismatchMessage(sparkType, arrowType));
                TinyIntVector tinyIntVector = (TinyIntVector) vector;
                return tinyIntVector.isNull(rowIndex) ? null : tinyIntVector.get(rowIndex);
            case SMALLINT:
                Preconditions.checkArgument(arrowType.equals(Types.MinorType.SMALLINT),
                        typeMismatchMessage(sparkType, arrowType));
                SmallIntVector smallIntVector = (SmallIntVector) vector;
                return smallIntVector.isNull(rowIndex) ? null : smallIntVector.get(rowIndex);
            case INT:
                Preconditions.checkArgument(arrowType.equals(Types.MinorType.INT),
                        typeMismatchMessage(sparkType, arrowType));
                IntVector intVector = (IntVector) vector;
                return intVector.isNull(rowIndex) ? null : intVector.get(rowIndex);
            case BIGINT:
                Preconditions.checkArgument(arrowType.equals(Types.MinorType.BIGINT),
                        typeMismatchMessage(sparkType, arrowType));
                BigIntVector bigIntVector = (BigIntVector) vector;
                return bigIntVector.isNull(rowIndex) ? null : bigIntVector.get(rowIndex);
            case FLOAT:
                Preconditions.checkArgument(arrowType.equals(Types.MinorType.FLOAT4),
                        typeMismatchMessage(sparkType, arrowType));
                Float4Vector float4Vector = (Float4Vector) vector;
                return float4Vector.isNull(rowIndex) ? null : float4Vector.get(rowIndex);
            case TIME:
            case DOUBLE:
                Preconditions.checkArgument(arrowType.equals(Types.MinorType.FLOAT8),
                        typeMismatchMessage(sparkType, arrowType));
                Float8Vector float8Vector = (Float8Vector) vector;
                return float8Vector.isNull(rowIndex) ? null : float8Vector.get(rowIndex);
            case BINARY:
                Preconditions.checkArgument(arrowType.equals(Types.MinorType.VARBINARY),
                        typeMismatchMessage(sparkType, arrowType));
                VarBinaryVector varBinaryVector = (VarBinaryVector) vector;
                return varBinaryVector.isNull(rowIndex) ? null : varBinaryVector.get(rowIndex);
            case DECIMAL:
                Preconditions.checkArgument(arrowType.equals(Types.MinorType.VARCHAR),
                        typeMismatchMessage(sparkType, arrowType));
                VarCharVector varCharVectorForDecimal = (VarCharVector) vector;
                if (varCharVectorForDecimal.isNull(rowIndex)) {
                    return null;
                }
                UTF8String decimalUtf8 = UTF8String.fromBytes(varCharVectorForDecimal.get(rowIndex));
                String decimalValue = decimalUtf8.toString();
                Decimal decimal = new Decimal();
                try {
                    decimal.set(new scala.math.BigDecimal(new BigDecimal(decimalValue)));
                } catch (NumberFormatException e) {
                    String errMsg = "Decimal response result '" + decimalValue + "' is illegal.";
                    logger.error(errMsg, e);
                    throw new StarRocksException(errMsg);
                }
                return decimal;
            case DECIMALV2:
            case DECIMAL32:
            case DECIMAL64:
            case DECIMAL128:
                Preconditions.checkArgument(arrowType.equals(Types.MinorType.DECIMAL),
                        typeMismatchMessage(sparkType, arrowType));
                DecimalVector decimalVector = (DecimalVector) vector;
                if (decimalVector.isNull(rowIndex)) {
                    return null;
                }
                return Decimal.apply(decimalVector.getObject(rowIndex));
            case DATE:
                Preconditions.checkArgument(arrowType.equals(Types.MinorType.VARCHAR),
                        typeMismatchMessage(sparkType, arrowType));
                VarCharVector varCharVectorForDate = (VarCharVector) vector;
                if (varCharVectorForDate.isNull(rowIndex)) {
                    return null;
                }
                String dateValue = new String(varCharVectorForDate.get(rowIndex), StandardCharsets.UTF_8);
                LocalDate parsedTime = LocalDate.parse(dateValue, dateFormatter);
                return Date.valueOf(parsedTime);
            case DATETIME:
                Preconditions.checkArgument(arrowType.equals(Types.MinorType.VARCHAR),
                        typeMismatchMessage(sparkType, arrowType));
                VarCharVector varCharVectorForDateTime = (VarCharVector) vector;
                if (varCharVectorForDateTime.isNull(rowIndex)) {
                    return null;
                }
                String dateTimeValue = new String(varCharVectorForDateTime.get(rowIndex), StandardCharsets.UTF_8);
                ZonedDateTime zonedDateTime = ZonedDateTime.parse(dateTimeValue, dateTimeFormatter);
                return Timestamp.from(zonedDateTime.toInstant());
            case LARGEINT:
            case VARCHAR:
            case CHAR:
                Preconditions.checkArgument(arrowType.equals(Types.MinorType.VARCHAR),
                        typeMismatchMessage(sparkType, arrowType));
                VarCharVector varCharVector = (VarCharVector) vector;
                if (varCharVector.isNull(rowIndex)) {
                    return null;
                }
                return UTF8String.fromBytes(varCharVector.get(rowIndex));
            case STRUCT:
                return convertStructValue(vector, arrowType, rowIndex, schemaField);
            case ARRAY:
                Preconditions.checkArgument(arrowType.equals(Types.MinorType.LIST),
                        typeMismatchMessage(sparkType, arrowType));
                return convertArrayValue(vector, arrowType, rowIndex, schemaField);
            case MAP:
                Preconditions.checkArgument(arrowType.equals(Types.MinorType.MAP),
                        typeMismatchMessage(sparkType, arrowType));
                return convertMapValue(vector, arrowType, rowIndex, schemaField);
            default:
                String srType = schemaField == null ? fieldType.getTypeName() : schemaField.getType();
                String errMsg = String.format("Unsupported or unexpected StarRocks type '%s', Arrow minor type '%s'. ",
                        srType, arrowType.name());
                logger.error(errMsg);
                throw new StarRocksException(errMsg);
        }
    }

    private Object convertStructValue(FieldVector vector,
                                      Types.MinorType arrowType,
                                      int rowIndex,
                                      StarRocksField schemaField) {
        Preconditions.checkArgument(arrowType.equals(Types.MinorType.STRUCT),
                "Spark type is STRUCT, but mapped type is " + arrowType.name());
        StructVector structVector = (StructVector) vector;
        if (structVector.isNull(rowIndex)) {
            return null;
        }
        List<FieldVector> children = structVector.getChildrenFromFields();
        String[] childTypes = FieldType.extractStructTypes(
                schemaField == null ? null : schemaField.getType());
        if (childTypes.length != children.size()) {
            throw new StarRocksException("The number of extracted subtypes (" + childTypes.length +
                    ") does not match the number of child fields (" + children.size() +
                    ") for the struct: " + (schemaField == null ? "null" : schemaField.getType()));
        }
        Object[] childValues = new Object[children.size()];
        for (int i = 0; i < children.size(); i++) {
            FieldVector childVector = children.get(i);
            Types.MinorType childType = childVector.getMinorType();
            FieldType childFieldType = FieldType.elegantOf(childTypes[i])
                    .orElse(FieldType.VARCHAR);
            StarRocksField childSchemaField = new StarRocksField(childVector.getName(), childTypes[i], i, null, null, null);
            childValues[i] = convertValue(childVector, childType, childFieldType, rowIndex, childSchemaField);
        }
        return childValues;
    }

    private Object convertArrayValue(FieldVector vector, Types.MinorType arrowType, int rowIndex, StarRocksField schemaField) {
        org.apache.arrow.vector.complex.ListVector listVector = (org.apache.arrow.vector.complex.ListVector) vector;
        if (listVector.isNull(rowIndex)) {
            return null;
        }
        int start = listVector.getElementStartIndex(rowIndex);
        int end = listVector.getElementEndIndex(rowIndex);
        FieldVector childVector = listVector.getDataVector();

        String[] extractedTypes = FieldType.extractArrayTypes(
                schemaField == null ? null : schemaField.getType());
        if (extractedTypes.length == 0) {
            throw new StarRocksException("Failed to extract array element type from schema field: " + 
                    (schemaField == null ? "null" : schemaField.getType()));
        }
        String childTypeStr = extractedTypes[0];

        StarRocksField valueSchemaField = new StarRocksField("array", childTypeStr, 0, null, null, null);

        List<Object> array = new java.util.ArrayList<>();
        for (int i = start; i < end; i++) {
            Types.MinorType childType = childVector.getMinorType();
            FieldType childFieldType = FieldType.elegantOf(childTypeStr).orElse(FieldType.VARCHAR);
            array.add(convertValue(childVector, childType, childFieldType, i, valueSchemaField));
        }
        return array;
    }

    private Object convertMapValue(FieldVector vector, Types.MinorType arrowType, int rowIndex, StarRocksField schemaField) {
        org.apache.arrow.vector.complex.MapVector mapVector = (org.apache.arrow.vector.complex.MapVector) vector;
        if (mapVector.isNull(rowIndex)) {
            return null;
        }
        int start = mapVector.getElementStartIndex(rowIndex);
        int end = mapVector.getElementEndIndex(rowIndex);
        FieldVector entryVector = mapVector.getDataVector();
        FieldVector keyVector = ((org.apache.arrow.vector.complex.StructVector) entryVector).getChild("key");
        FieldVector valueVector = ((org.apache.arrow.vector.complex.StructVector) entryVector).getChild("value");

        String[] extractedTypes = FieldType.extractMapTypes(
                schemaField == null ? null : schemaField.getType());
        if (extractedTypes.length < 2) {
            throw new StarRocksException("Failed to extract map key/value types from schema field: " + 
                    (schemaField == null ? "null" : schemaField.getType()));
        }

        StarRocksField keySchemaField = new StarRocksField("key", extractedTypes[0], 0, null, null, null);
        StarRocksField valueSchemaField = new StarRocksField("value", extractedTypes[1], 1, null, null, null);

        java.util.Map<Object, Object> map = new java.util.LinkedHashMap<>();
        for (int i = start; i < end; i++) {
            Types.MinorType keyType = keyVector.getMinorType();
            FieldType keyFieldType = FieldType.elegantOf(extractedTypes[0]).orElse(FieldType.VARCHAR);
            Object key = convertValue(keyVector, keyType, keyFieldType, i, keySchemaField);

            Types.MinorType valueType = valueVector.getMinorType();
            FieldType valueFieldType = FieldType.elegantOf(extractedTypes[1]).orElse(FieldType.VARCHAR);
            Object value = convertValue(valueVector, valueType, valueFieldType, i, valueSchemaField);

            map.put(key, value);
        }
        return map;
    }

    public void close() {
        try {
            if (root != null) {
                root.close();
            }
            if (arrowStreamReader != null) {
                arrowStreamReader.close();
            }
            if (rootAllocator != null) {
                rootAllocator.close();
            }
        } catch (Exception e) {
            logger.warn("Error closing Arrow resources", e);
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
