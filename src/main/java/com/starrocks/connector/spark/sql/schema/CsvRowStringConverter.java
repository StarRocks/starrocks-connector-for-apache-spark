package com.starrocks.connector.spark.sql.schema;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CsvRowStringConverter extends AbstractRowStringConverter {

    private final String separator;

    public CsvRowStringConverter(StructType schema, String separator) {
        super(schema);
        this.separator = separator;
    }

    @Override
    public String fromRow(Row row) {
        if (row.schema() == null) {
            throw new RuntimeException("Can't convert Row without schema");
        }
        String[] data = new String[row.length()];
        for (int i = 0; i < row.length(); i++) {
            if (!row.isNullAt(i)) {
                StructField field = row.schema().fields()[i];
                data[i] = convert(field.dataType(), row.get(i)).toString();
            }
        }

        StringBuilder sb = new StringBuilder();
        for (int idx = 0; idx < data.length; idx++) {
            Object val = data[idx];
            sb.append(null == val ? "\\N" : val);
            if (idx < data.length - 1) {
                sb.append(separator);
            }
        }
        return sb.toString();
    }
}
