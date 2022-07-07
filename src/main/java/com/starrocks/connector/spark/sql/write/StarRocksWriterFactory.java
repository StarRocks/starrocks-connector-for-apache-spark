package com.starrocks.connector.spark.sql.write;

import com.starrocks.connector.spark.sql.conf.WriteStarRocksConfig;
import com.starrocks.connector.spark.sql.schema.RowStringConverter;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.DataWriterFactory;
import org.apache.spark.sql.connector.write.streaming.StreamingDataWriterFactory;

public class StarRocksWriterFactory implements DataWriterFactory, StreamingDataWriterFactory {

    private final RowStringConverter converter;
    private final WriteStarRocksConfig config;

    public StarRocksWriterFactory(RowStringConverter converter, WriteStarRocksConfig config) {
        this.converter = converter;
        this.config = config;
    }

    @Override
    public DataWriter<InternalRow> createWriter(int partitionId, long taskId) {
        return new StarRocksDataWriter(config, converter, partitionId, taskId, -1);
    }

    @Override
    public DataWriter<InternalRow> createWriter(int partitionId, long taskId, long epochId) {
        return new StarRocksDataWriter(config, converter, partitionId, taskId, epochId);
    }
}
