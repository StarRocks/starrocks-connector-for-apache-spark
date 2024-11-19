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
