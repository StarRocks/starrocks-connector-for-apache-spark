package com.starrocks.connector.spark.sql.write;

import com.starrocks.connector.spark.sql.conf.WriteStarRocksConfig;
import com.starrocks.connector.spark.sql.schema.RowStringConverter;
import org.apache.spark.sql.connector.write.*;
import org.apache.spark.sql.connector.write.streaming.StreamingDataWriterFactory;
import org.apache.spark.sql.connector.write.streaming.StreamingWrite;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StarRocksWrite implements BatchWrite, StreamingWrite {

    private static final Logger log = LoggerFactory.getLogger(StarRocksWrite.class);

    private final LogicalWriteInfo info;
    private final WriteStarRocksConfig config;
    private final RowStringConverter converter;

    public StarRocksWrite(LogicalWriteInfo info, WriteStarRocksConfig config, RowStringConverter converter) {
        this.info = info;
        this.config = config;
        this.converter = converter;
    }

    @Override
    public DataWriterFactory createBatchWriterFactory(PhysicalWriteInfo info) {
        return new StarRocksWriterFactory(converter, config);
    }

    @Override
    public void commit(WriterCommitMessage[] messages) {
        log.info("batch query `{}` commit", info.queryId());
    }

    @Override
    public void abort(WriterCommitMessage[] messages) {
        log.info("batch query `{}` abort", info.queryId());
    }

    @Override
    public StreamingDataWriterFactory createStreamingWriterFactory(PhysicalWriteInfo info) {
        return new StarRocksWriterFactory(converter, config);
    }

    @Override
    public void commit(long epochId, WriterCommitMessage[] messages) {
        log.info("streaming query `{}` commit", info.queryId());
    }

    @Override
    public void abort(long epochId, WriterCommitMessage[] messages) {
        log.info("streaming query `{}` abort", info.queryId());
    }
}
