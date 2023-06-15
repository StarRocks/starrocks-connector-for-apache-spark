package com.starrocks.connector.spark.sql.write;

import com.starrocks.connector.spark.sql.conf.WriteStarRocksConfig;
import com.starrocks.connector.spark.sql.schema.CsvRowStringConverter;
import com.starrocks.connector.spark.sql.schema.JSONRowStringConverter;
import com.starrocks.connector.spark.sql.schema.RowStringConverter;
import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.connector.write.streaming.StreamingWrite;

public class StarRocksWriteBuilder implements WriteBuilder {
    private final LogicalWriteInfo info;
    private final WriteStarRocksConfig config;
    private final RowStringConverter converter;

    public StarRocksWriteBuilder(LogicalWriteInfo info, WriteStarRocksConfig config) {
        this.info = info;
        this.config = config;

        RowStringConverter converter;
        if ("csv".equalsIgnoreCase(config.getFormat())) {
            converter = new CsvRowStringConverter(info.schema(), config.getColumnSeparator());
        }  else if ("json".equalsIgnoreCase(config.getFormat())) {
            converter = new JSONRowStringConverter(info.schema());
        } else {
            throw new RuntimeException("UnSupport format " + config.getFormat());
        }
        this.converter = converter;
    }

    @Override
    public BatchWrite buildForBatch() {
        return new StarRocksWrite(info, config, converter);
    }

    @Override
    public StreamingWrite buildForStreaming() {
        return new StarRocksWrite(info, config, converter);
    }
}
