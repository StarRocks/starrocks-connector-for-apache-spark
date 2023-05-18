package com.starrocks.connector.spark.sql.schema;

import com.alibaba.fastjson2.JSONObject;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.*;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.io.Serializable;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

public abstract class AbstractRowStringConverter implements RowStringConverter, Serializable {

    private final SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd");

    private final Function<InternalRow, Row> internalRowConverter;

    public AbstractRowStringConverter(StructType schema) {
        this(new InternalRowToRowFunction(schema));
    }

    public AbstractRowStringConverter(Function<InternalRow, Row> internalRowConverter) {
        this.internalRowConverter = internalRowConverter;
    }

    @Override
    public String fromRow(InternalRow row) {
        return fromRow(internalRowConverter.apply(row));
    }

    protected Object convert(DataType dataType, Object data) {
        try {
            if (DataTypes.StringType.acceptsType(dataType)
                    || DataTypes.BooleanType.acceptsType(dataType)
                    || DataTypes.DoubleType.acceptsType(dataType)
                    || DataTypes.FloatType.acceptsType(dataType)
                    || DataTypes.ByteType.acceptsType(dataType)
                    || DataTypes.IntegerType.acceptsType(dataType)
                    || DataTypes.LongType.acceptsType(dataType)
                    || DataTypes.ShortType.acceptsType(dataType)
                    || DataTypes.NullType.acceptsType(dataType)) {
                return data;
            } else if (DataTypes.DateType.acceptsType(dataType)) {
                return dateFormatter.format((Date) data);
            } else if (DataTypes.TimestampType.acceptsType(dataType)) {
                return ((Timestamp) data).toLocalDateTime().toString();
            } else if (dataType instanceof DecimalType) {
                return data instanceof BigDecimal
                        ? (BigDecimal) data
                        : ((Decimal) data).toBigDecimal().bigDecimal();
            } else if (dataType instanceof ArrayType) {
                DataType elementType = ((ArrayType) dataType).elementType();
                List<Object> dataList;
                if (data instanceof Collection) {
                    dataList = new ArrayList<>((Collection<?>) data);
                } else {
                    dataList = JavaConverters.seqAsJavaList((Seq<Object>) data);
                }

                return dataList.stream().map(d -> convert(elementType, d)).collect(Collectors.toList());
            } else if (dataType instanceof MapType) {
                DataType keyType = ((MapType) dataType).keyType();
                DataType valueType = ((MapType) dataType).valueType();
                if (!(keyType instanceof StringType)) {
                    throw new RuntimeException(String.format("Can't cast %s. Invalid key type %s.", data, keyType));
                }

                Map<String, Object> dataMap;
                if (data instanceof Map) {
                    dataMap = (Map<String, Object>) data;
                } else {
                    dataMap = JavaConverters.mapAsJavaMap((scala.collection.Map<String, Object>) data);
                }

                return dataMap.entrySet().stream().collect(
                        Collectors.toMap(
                                Map.Entry::getKey,
                                entry -> convert(valueType, entry.getValue())
                        )
                );
            } else if (dataType instanceof StructType) {
                Row row = (Row) data;
                JSONObject jsonObject = new JSONObject();
                for (StructField field : row.schema().fields()) {
                    int idx = row.fieldIndex(field.name());
                    if (!(field.nullable() && row.isNullAt(idx))) {
                        jsonObject.put(field.name(), convert(field.dataType(), row.get(idx)));
                    }
                }
                return jsonObject;
            } else if (dataType instanceof BinaryType) {
                return data;
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        throw new RuntimeException(String.format("Can't cast %s, Invalid type %s", data, dataType));
    }
}
