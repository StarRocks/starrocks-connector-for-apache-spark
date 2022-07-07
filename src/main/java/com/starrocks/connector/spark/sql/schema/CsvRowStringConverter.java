package com.starrocks.connector.spark.sql.schema;

import com.starrocks.connector.spark.sql.conf.WriteStarRocksConfig;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class CsvRowStringConverter extends AbstractRowStringConverter {

    private final String separator;

    public CsvRowStringConverter(WriteStarRocksConfig config, StructType schema) {
        super(schema);
        this.separator = config.getColumnSeparator();
    }

    @Override
    public String fromRow(Row row) {
        if (row.schema() == null) {
            throw new RuntimeException("Can't convert Row without schema");
        }
        String[] data = new String[row.length()];

        for (StructField field : row.schema().fields()) {
            int idx = row.fieldIndex(field.name());
            if (field.nullable() && row.isNullAt(idx)) {
                data[idx] = null;
            } else {
                data[idx] = convert(field.dataType(), row.get(idx)).toString();
            }
        }

        return String.join(separator, data);
    }
}
