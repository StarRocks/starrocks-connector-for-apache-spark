package com.starrocks.connector.spark.sql.write;

import com.starrocks.connector.spark.sql.conf.WriteStarRocksConfig;
import com.starrocks.connector.spark.sql.schema.RowStringConverter;
import com.starrocks.connector.spark.util.EnvUtils;
import com.starrocks.data.load.stream.StreamLoadManager;
import com.starrocks.data.load.stream.StreamLoadSnapshot;
import com.starrocks.data.load.stream.v2.StreamLoadManagerV2;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;

public class StarRocksDataWriter implements DataWriter<InternalRow>, Serializable {
    private static final Logger log = LoggerFactory.getLogger(StarRocksDataWriter.class);

    private final WriteStarRocksConfig config;
    private final RowStringConverter converter;
    private final int partitionId;
    private final long taskId;
    private final long epochId;

    private final StreamLoadManager manager;

    public StarRocksDataWriter(WriteStarRocksConfig config,
                               RowStringConverter converter,
                               int partitionId,
                               long taskId,
                               long epochId) {
        this.config = config;
        this.converter = converter;
        this.partitionId = partitionId;
        this.taskId = taskId;
        this.epochId = epochId;
        this.manager = new StreamLoadManagerV2(config.toStreamLoadProperties(), true);
    }

    public void open() {
        manager.init();
        log.info("Open data writer for partition: {}, task: {}, epoch: {}, {}",
                partitionId, taskId, epochId, EnvUtils.getGitInformation());
    }

    @Override
    public void write(InternalRow internalRow) throws IOException {
        String data = converter.fromRow(internalRow);
        manager.write(null, config.getDatabase(), config.getTable(), data);

        log.debug("partitionId: {}, taskId: {}, epochId: {}, receive raw row: {}",
                partitionId, taskId, epochId, internalRow);
        log.debug("partitionId: {}, taskId: {}, epochId: {}, receive converted row: {}",
                partitionId, taskId, epochId, data);
    }

    @Override
    public WriterCommitMessage commit() throws IOException {
        log.info("partitionId: {}, taskId: {}, epochId: {} commit", partitionId, taskId, epochId);
        try {
            manager.flush();
            return new StarRocksWriterCommitMessage(partitionId, taskId, epochId, null);
        } catch (Exception e) {
            String errMsg = String.format("Failed to commit, partitionId: %s, taskId: %s, epochId: %s",
                    partitionId, taskId, epochId);
            log.error("{}", errMsg, e);
            throw new IOException(errMsg, e);
        }
    }

    @Override
    public void abort() throws IOException {
        log.info("partitionId: {}, taskId: {}, epochId: {} abort", partitionId, taskId, epochId);
        StreamLoadSnapshot snapshot = manager.snapshot();
        try {
            boolean success = manager.abort(snapshot);;
            if (success) {
                return;
            }
            throw new IOException("abort not successful");
        } catch (Exception e) {
            String errMsg = String.format("Failed to abort, partitionId: %s, taskId: %s, epochId: %s",
                    partitionId, taskId, epochId);
            log.error("{}", errMsg, e);
            throw new IOException(errMsg, e);
        }
    }

    @Override
    public void close() throws IOException {
        log.info("partitionId: {}, taskId: {}, epochId: {} close", partitionId, taskId, epochId);
        manager.close();
    }
}
