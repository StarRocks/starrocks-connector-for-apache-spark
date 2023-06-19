package com.starrocks.connector.spark.sql.schema;

import com.alibaba.fastjson2.JSONObject;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.Serializable;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.function.Function;

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
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        throw new RuntimeException(String.format("Can't cast %s, Invalid type %s", data, dataType));
    }
}
