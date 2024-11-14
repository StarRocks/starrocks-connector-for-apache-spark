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

import com.starrocks.connector.spark.exception.NotSupportedOperationException;
import com.starrocks.connector.spark.rest.models.PartitionType;
import com.starrocks.connector.spark.sql.conf.WriteStarRocksConfig;
import com.starrocks.connector.spark.sql.connect.StarRocksConnector;
import org.apache.spark.sql.connector.write.*;
import org.apache.spark.sql.connector.write.streaming.StreamingDataWriterFactory;
import org.apache.spark.sql.connector.write.streaming.StreamingWrite;
import org.apache.spark.sql.sources.AlwaysTrue;
import org.apache.spark.sql.sources.Filter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

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
        createTemporaryPartitionOrTable(config);
        return new StarRocksWriterFactory(logicalInfo.schema(), config);
    }

    private boolean isOverwriteTable(Filter[] filters) {
        return  filters.length == 0
                || Arrays.stream(filters).allMatch(AlwaysTrue.class::isInstance);
    }

    private void createTemporaryPartitionOrTable(WriteStarRocksConfig config) {
        if (!config.isOverwrite()) {
            return;
        }
        String table = config.getTable();

        Filter[] filters = config.getFilters();
        if (isOverwriteTable(filters)
            && config.getOverwriteTempPartitions().isEmpty()) {
            String tempTable = table + WriteStarRocksConfig.TEMPORARY_TABLE_SUFFIX;
            StarRocksConnector.createTempTable(config, tempTable);
            config.setTempTableName(tempTable);
        } else {
            PartitionType partitionType = StarRocksConnector.getPartitionType(config);
            if (PartitionType.NONE.equals(partitionType)) {
                throw new NotSupportedOperationException("Overwriting partition only supports list/range partitioning," +
                    " not support none partitioning table !!!");
            }

            if (PartitionType.EXPRESSION.equals(partitionType)) {
                throw new NotSupportedOperationException("Overwriting partition only supports list/range partitioning," +
                    " not support expression/automatic partitioning !!!");
            }
            config.getOverwriteTempPartitions().forEach((tempPartition, partitionExpr) -> {
                String overwritePartition = config.getOverwriteTempPartitionMappings().get(tempPartition);
                StarRocksConnector.dropAndCreatePartition(
                    config, tempPartition, partitionExpr, partitionType, overwritePartition);
            });
        }
    }

    @Override
    public boolean useCommitCoordinator() {
        return true;
    }

    @Override
    public void commit(WriterCommitMessage[] messages) {
        log.info("batch query `{}` commit", logicalInfo.queryId());
        if (config.isOverwrite() && config.getTempTableName() != null) {
            StarRocksConnector.swapTable(config, config.getTempTableName());
            StarRocksConnector.dropTable(config, config.getTempTableName());
        } else if (config.isOverwrite() && !config.getOverwritePartitions().isEmpty()) {
            PartitionType partitionType = StarRocksConnector.getPartitionType(config);
            boolean dynamicPartitionTable = StarRocksConnector.isDynamicPartitionTable(config);
            if (!dynamicPartitionTable) {
                config.getOverwritePartitions().forEach((partitionName, partitionValue) -> {
                    StarRocksConnector.createPartition(config, partitionName, partitionValue, partitionType);
                });
            } else {
                log.info("no need create partition for dynamic partition table");
            }
            config.getOverwriteTempPartitionMappings().forEach((tempPartitionName, partitionName) -> {
                StarRocksConnector.replacePartition(config, partitionName, tempPartitionName);
            });
        }
    }

    @Override
    public void abort(WriterCommitMessage[] messages) {
        log.info("batch query `{}` abort", logicalInfo.queryId());
        if (config.isOverwrite() && config.getTempTableName() != null) {
            StarRocksConnector.dropTable(config, config.getTempTableName());
        } else if (config.isOverwrite() && !config.getOverwritePartitions().isEmpty()) {
            config.getOverwriteTempPartitions().keySet().forEach(tempPartition -> {
                StarRocksConnector.dropTemporaryPartition(config, tempPartition);
            });
        }
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
