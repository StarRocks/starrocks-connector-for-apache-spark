package com.starrocks.connector.spark.sql.write;

import com.starrocks.connector.spark.sql.conf.WriteStarRocksConfig;
import com.starrocks.connector.spark.sql.schema.RowStringConverter;
import com.starrocks.data.load.stream.DefaultStreamLoadManager;
import com.starrocks.data.load.stream.StreamLoadManager;
import com.starrocks.data.load.stream.StreamLoadSnapshot;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

public class StarRocksDataWriter implements DataWriter<InternalRow>, Serializable {

    private static final Logger log = LoggerFactory.getLogger(StarRocksDataWriter.class);

    private final WriteStarRocksConfig config;
    private final RowStringConverter converter;
    private final int partitionId;
    private final long taskId;
    private final long epochId;

    private final StreamLoadManager manager;
    private final AtomicBoolean managerInit = new AtomicBoolean(false);

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
        this.manager = new DefaultStreamLoadManager(config.toStreamLoadProperties());
    }

    @Override
    public void write(InternalRow internalRow) throws IOException {
        if (managerInit.compareAndSet(false, true)) {
            manager.init();
        }
        manager.write(null, config.getDatabase(), config.getTable(), converter.fromRow(internalRow));
    }

    @Override
    public WriterCommitMessage commit() throws IOException {
        log.info("pid: {}, taskId: {}, epochId: {} commit", partitionId, taskId, epochId);
        manager.flush();
        StreamLoadSnapshot snapshot = manager.snapshot();
        manager.commit(snapshot);
        return new StarRocksWriterCommitMessage(partitionId, taskId, epochId, null);
    }

    @Override
    public void abort() throws IOException {
        log.info("pid: {}, taskId: {}, epochId: {} abort", partitionId, taskId, epochId);
        StreamLoadSnapshot snapshot = manager.snapshot();
        manager.abort(snapshot);
        manager.close();
    }

    @Override
    public void close() throws IOException {
        log.info("pid: {}, taskId: {}, epochId: {} close", partitionId, taskId, epochId);
        if (managerInit.compareAndSet(true, false)) {
            manager.close();
        }
    }
}
