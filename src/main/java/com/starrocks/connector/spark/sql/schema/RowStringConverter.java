package com.starrocks.connector.spark.sql.schema;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.InternalRow;

public interface RowStringConverter {
    String fromRow(InternalRow row);
    String fromRow(Row row);
}
