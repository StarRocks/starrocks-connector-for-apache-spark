package com.starrocks.connector.spark.sql.write;

import com.starrocks.connector.spark.sql.conf.WriteStarRocksConfig;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.DataWriterFactory;
import org.apache.spark.sql.connector.write.streaming.StreamingDataWriterFactory;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StarRocksWriterFactory implements DataWriterFactory, StreamingDataWriterFactory {

    private static final Logger LOG = LoggerFactory.getLogger(StarRocksWriterFactory.class);

    private final StructType schema;
    private final WriteStarRocksConfig config;

    public StarRocksWriterFactory(StructType schema, WriteStarRocksConfig config) {
        this.schema = schema;
        this.config = config;
    }

    @Override
    public DataWriter<InternalRow> createWriter(int partitionId, long taskId) {
        return createAndOpenWriter(partitionId, taskId, -1);
    }

    @Override
    public DataWriter<InternalRow> createWriter(int partitionId, long taskId, long epochId) {
        return createAndOpenWriter(partitionId, taskId, epochId);
    }

    private StarRocksDataWriter createAndOpenWriter(int partitionId, long taskId, long epochId) {
        StarRocksDataWriter writer = new StarRocksDataWriter(config, schema, partitionId, taskId, epochId);
        try {
            writer.open();
        } catch (Exception e) {
            String errMsg = String.format("Failed to open writer for " +
                            "partition: %s, task: %s, epoch: %s", partitionId, taskId, epochId);
            LOG.error("{}", errMsg, e);
            try {
                writer.close();
            } catch (Exception ce) {
                LOG.error("Failed to close writer for partition: {}, task: {}, epoch: {}", partitionId, taskId, epochId, ce);
            }

            throw new RuntimeException(errMsg, e);
        }
        return writer;
    }
}
