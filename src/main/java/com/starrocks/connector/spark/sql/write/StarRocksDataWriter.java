package com.starrocks.connector.spark.sql.write;

import com.starrocks.connector.spark.sql.conf.WriteStarRocksConfig;
import com.starrocks.connector.spark.sql.schema.RowStringConverter;
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
import java.util.concurrent.atomic.AtomicBoolean;

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
        this.manager = new StreamLoadManagerV2(config.toStreamLoadProperties(), true);
    }

    @Override
    public void write(InternalRow internalRow) throws IOException {
        if (managerInit.compareAndSet(false, true)) {
            manager.init();
        }

        String data = converter.fromRow(internalRow);
        manager.write(null, config.getDatabase(), config.getTable(), data);

        log.debug("Receive raw row: {}", internalRow);
        log.debug("Receive converted row: {}", data);
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
