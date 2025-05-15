// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.connector.spark.sql.write;

import com.starrocks.connector.spark.sql.conf.WriteStarRocksConfig;
import com.starrocks.connector.spark.sql.schema.CsvRowStringConverter;
import com.starrocks.connector.spark.sql.schema.JSONRowStringConverter;
import com.starrocks.connector.spark.sql.schema.RowStringConverter;
import com.starrocks.connector.spark.util.EnvUtils;
import com.starrocks.data.load.stream.StreamLoadManager;
import com.starrocks.data.load.stream.StreamLoadSnapshot;
import com.starrocks.data.load.stream.v2.StreamLoadManagerV2;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.io.Serializable;

public class StarRocksDataWriter implements DataWriter<InternalRow>, Serializable {
    private static final Logger log = LoggerFactory.getLogger(StarRocksDataWriter.class);

    private final WriteStarRocksConfig config;
    private final int partitionId;
    private final long taskId;
    private final long epochId;
    private final RowStringConverter converter;
    private final StreamLoadManager manager;
    public StarRocksDataWriter(WriteStarRocksConfig config,
                               StructType schema,
                               int partitionId,
                               long taskId,
                               long epochId) {
        this.config = config;
        this.partitionId = partitionId;
        this.taskId = taskId;
        this.epochId = epochId;
        if ("csv".equalsIgnoreCase(config.getFormat())) {
            this.converter = new CsvRowStringConverter(schema, config.getColumnSeparator(), config.getTimeZone());
        }  else if ("json".equalsIgnoreCase(config.getFormat())) {
            this.converter = new JSONRowStringConverter(
                    schema, config.getStreamLoadColumnNames(), config.getStarRocksJsonColumnNames(), config.getTimeZone());
        } else {
            throw new RuntimeException("Unsupported format " + config.getFormat());
        }
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
