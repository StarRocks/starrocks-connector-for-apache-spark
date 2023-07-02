package com.starrocks.connector.spark.sql.schema;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class JSONRowStringConverter extends AbstractRowStringConverter {

    private static final Logger LOG = LoggerFactory.getLogger(JSONRowStringConverter.class);

    private final ObjectMapper mapper;

    public JSONRowStringConverter(StructType schema) {
        super(schema);
        this.mapper = new ObjectMapper();
    }

    @Override
    public String fromRow(Row row) {
        if (row.schema() == null) {
            throw new RuntimeException("Can't convert Row without schema");
        }

        Map<String, Object> data = new HashMap<>();
        for (StructField field : row.schema().fields()) {
            int idx = row.fieldIndex(field.name());
            if (!(field.nullable() && row.isNullAt(idx))) {
                data.put(field.name(), convert(field.dataType(), row.get(idx)));
            }
        }

        try {
            return mapper.writeValueAsString(data);
        } catch (Exception e) {
            LOG.error("Failed to serialize row to json, data: {}", data, e);
            throw new RuntimeException(e);
        }
    }
}
