package com.starrocks.connector.spark.sql.write;

import com.starrocks.connector.spark.sql.conf.WriteStarRocksConfig;
import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.DataWriterFactory;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.PhysicalWriteInfo;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.connector.write.streaming.StreamingDataWriterFactory;
import org.apache.spark.sql.connector.write.streaming.StreamingWrite;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StarRocksWrite implements BatchWrite, StreamingWrite {

    private static final Logger log = LoggerFactory.getLogger(StarRocksWrite.class);

    private final LogicalWriteInfo logicalInfo;
    private final WriteStarRocksConfig config;

    public StarRocksWrite(LogicalWriteInfo logicalInfo, WriteStarRocksConfig config) {
        this.logicalInfo = logicalInfo;
        this.config = config;
    }

    @Override
    public DataWriterFactory createBatchWriterFactory(PhysicalWriteInfo info) {
        return new StarRocksWriterFactory(logicalInfo.schema(), config);
    }

    @Override
    public void commit(WriterCommitMessage[] messages) {
        log.info("batch query `{}` commit", logicalInfo.queryId());
    }

    @Override
    public void abort(WriterCommitMessage[] messages) {
        log.info("batch query `{}` abort", logicalInfo.queryId());
    }

    @Override
    public StreamingDataWriterFactory createStreamingWriterFactory(PhysicalWriteInfo info) {
        return new StarRocksWriterFactory(logicalInfo.schema(), config);
    }

    @Override
    public void commit(long epochId, WriterCommitMessage[] messages) {
        log.info("streaming query `{}` commit", logicalInfo.queryId());
    }

    @Override
    public void abort(long epochId, WriterCommitMessage[] messages) {
        log.info("streaming query `{}` abort", logicalInfo.queryId());
    }
}
