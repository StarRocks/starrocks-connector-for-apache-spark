package com.starrocks.connector.spark.sql.schema;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;

public class JSONRowStringConverter extends AbstractRowStringConverter {

    public JSONRowStringConverter(StructType schema) {
        super(schema);
    }

    @Override
    public String fromRow(Row row) {
        if (row.schema() == null) {
            throw new RuntimeException("Can't convert Row without schema");
        }
        return convert(row.schema(), row).toString();
    }
}
