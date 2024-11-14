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

import java.text.SimpleDateFormat;
import java.util.Date;

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

    private void createTemporaryPartitionOrTable(WriteStarRocksConfig config) {
        if (!config.isOverwrite()) {
            return;
        }
        String database = config.getDatabase();
        String table = config.getTable();

        Filter[] filters = config.getFilters();
        if (filters.length == 1 && filters[0] instanceof AlwaysTrue
            && config.getOverwriteTempPartitions().isEmpty()) {
            SimpleDateFormat format = new SimpleDateFormat("yyyyMMddHHmmss");
            String tempTable = table + WriteStarRocksConfig.TEMPORARY_PARTITION_SUFFIX + format.format(new Date());
            String createTempTableDDL = "CREATE TABLE " + database + "." + tempTable +" LIKE " + database + "." + table;
            StarRocksConnector.createTableBySql(config, createTempTableDDL);
            config.setTempTableName(tempTable);
        } else if (config.isOverwrite() && !config.getOverwriteTempPartitions().isEmpty()) {
            PartitionType partitionType = StarRocksConnector.getPartitionType(config);
            if (PartitionType.NONE.equals(partitionType)) {
                throw new NotSupportedOperationException("Overwriting partition only supports list/range partitioning," +
                    " not support none partitioning table !!!");
            } else if (PartitionType.EXPRESSION.equals(partitionType)) {
                throw new NotSupportedOperationException("Overwriting partition only supports list/range partitioning," +
                    " not support expression/automatic partitioning !!!");
            }
            config.getOverwriteTempPartitions().forEach((tempPartition, partitionExpr) -> {
                String addTempPartitionDDL = getAddTemporaryPartitionDDL(config, tempPartition, partitionExpr, partitionType);
                try {
                    StarRocksConnector.createTemporaryPartitionBySql(config, addTempPartitionDDL);
                } catch (Exception e) {
                    if (e.getMessage().contains("Duplicate partition")) {
                        String overwritePartition = config.getOverwriteTempPartitionMappings().get(tempPartition);
                        StarRocksConnector.dropAndCreatePartitionBySql(config, addTempPartitionDDL, overwritePartition);
                    } else {
                        throw e;
                    }
                }
            });
        }
    }

    private static String getAddTemporaryPartitionDDL(
        WriteStarRocksConfig config, String tempPartition, String partitionExpr, PartitionType partitionType) {
        String addTemporaryPartitionDDL;
        if (PartitionType.LIST.equals(partitionType)) { // list partition
            addTemporaryPartitionDDL = String.format("ALTER TABLE `%s`.`%s` ADD TEMPORARY PARTITION %s VALUES IN %s",
                config.getDatabase(), config.getTable(), tempPartition, partitionExpr);
        } else { // range partition
            addTemporaryPartitionDDL = String.format("ALTER TABLE `%s`.`%s` ADD TEMPORARY PARTITION %s VALUES %s",
                config.getDatabase(), config.getTable(), tempPartition, partitionExpr);
        }
        return addTemporaryPartitionDDL;
    }

    @Override
    public boolean useCommitCoordinator() {
        return true;
    }

    @Override
    public void commit(WriterCommitMessage[] messages) {
        log.info("batch query `{}` commit", logicalInfo.queryId());
        if (config.isOverwrite() && config.getTempTableName() != null) {
            StarRocksConnector.swapTableBySql(config,
                "ALTER TABLE " + config.getDatabase() +"." + config.getTable() + " SWAP WITH " + config.getTempTableName());
            StarRocksConnector.dropTableBySql(config,
                "DROP TABLE IF EXISTS " + config.getDatabase() + "." + config.getTempTableName() + " FORCE");
        } else if (config.isOverwrite() && !config.getOverwritePartitions().isEmpty()) {
            PartitionType partitionType = StarRocksConnector.getPartitionType(config);
            boolean dynamicPartitionTable = StarRocksConnector.isDynamicPartitionTable(config);
            if (!dynamicPartitionTable) {
                config.getOverwritePartitions().forEach((partitionName, partitionValue) -> {
                    String addPartitionDDL;
                    if (PartitionType.LIST.equals(partitionType)) {
                        addPartitionDDL = String.format("ALTER TABLE `%s`.`%s` ADD PARTITION IF NOT EXISTS %s VALUES IN %s",
                            config.getDatabase(), config.getTable(), partitionName, partitionValue);
                    } else {
                        addPartitionDDL = String.format("ALTER TABLE `%s`.`%s` ADD PARTITION IF NOT EXISTS %s VALUES %s",
                            config.getDatabase(), config.getTable(), partitionName, partitionValue);
                    }
                    StarRocksConnector.createPartitionBySql(config, addPartitionDDL);
                });
            } else {
                log.info("no need create partition for dynamic partition table");
            }
            config.getOverwriteTempPartitionMappings().forEach((tempPartitionName, partitionName) -> {
                String replacePartitionDDL = String.format(
                    "ALTER TABLE `%s`.`%s` REPLACE PARTITION (`%s`) WITH TEMPORARY PARTITION (`%s`)",
                    config.getDatabase(), config.getTable(), partitionName, tempPartitionName);
                StarRocksConnector.replacePartitionBySql(config, replacePartitionDDL);
            });
        }
    }

    @Override
    public void abort(WriterCommitMessage[] messages) {
        log.info("batch query `{}` abort", logicalInfo.queryId());
        if (config.isOverwrite() && config.getTempTableName() != null) {
            StarRocksConnector.dropTableBySql(config,
                "DROP TABLE IF EXISTS " + config.getDatabase() + "." + config.getTempTableName() + " FORCE");
        } else if (config.isOverwrite() && !config.getOverwritePartitions().isEmpty()) {
            config.getOverwriteTempPartitions().keySet().forEach(tempPartition -> {
                String dropTempPartitionDDL = String.format(
                    "ALTER TABLE `%s`.`%s` DROP TEMPORARY PARTITION IF EXISTS %s",
                    config.getDatabase(), config.getTable(), tempPartition);
                StarRocksConnector.dropTempPartitionBySql(config, dropTempPartitionDDL);
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
