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
import org.apache.spark.sql.connector.distributions.Distribution;
import org.apache.spark.sql.connector.distributions.Distributions;
import org.apache.spark.sql.connector.expressions.Expression;
import org.apache.spark.sql.connector.expressions.Expressions;
import org.apache.spark.sql.connector.expressions.SortOrder;
import org.apache.spark.sql.connector.write.*;
import org.apache.spark.sql.connector.write.streaming.StreamingWrite;
import org.apache.spark.sql.sources.AlwaysTrue;
import org.apache.spark.sql.sources.Filter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StarRocksWriteBuilder implements WriteBuilder, SupportsOverwrite, SupportsDynamicOverwrite {

    private Logger logger = LoggerFactory.getLogger(getClass());
    private final LogicalWriteInfo info;
    private final WriteStarRocksConfig config;

    public StarRocksWriteBuilder(LogicalWriteInfo info, WriteStarRocksConfig config) {
        this.info = info;
        this.config = config;
    }

    @Override
    public Write build() {
        return new StarRocksWriteImpl(info, config);
    }

    @Override
    public WriteBuilder overwrite(Filter[] filters) {
        logger.info("invoke overwrite ....");
        config.setOverwrite(true);
        config.setFilters(filters);
        return this;
    }

    @Override
    public WriteBuilder overwriteDynamicPartitions() {
        logger.info("invoke overwriteDynamicPartitions ....");
        config.setOverwrite(true);
        config.setFilters(new Filter[]{new AlwaysTrue()});
        return this;
    }

    private static class StarRocksWriteImpl implements Write, RequiresDistributionAndOrdering {

        private final LogicalWriteInfo info;
        private final WriteStarRocksConfig config;

        public StarRocksWriteImpl(LogicalWriteInfo info, WriteStarRocksConfig config) {
            this.info = info;
            this.config = config;
        }

        @Override
        public String description() {
            return String.format("StarRocksWriteImpl[%s.%s]", config.getDatabase(), config.getTable());
        }

        @Override
        public BatchWrite toBatch() {
            return new StarRocksWrite(info, config);
        }

        @Override
        public StreamingWrite toStreaming() {
            return new StarRocksWrite(info, config);
        }

        @Override
        public int requiredNumPartitions() {
            return config.getNumPartitions();
        }

        @Override
        public Distribution requiredDistribution() {
            if (config.getNumPartitions() <= 0) {
                return Distributions.unspecified();
            }

            // TODO is it possible to implement a distribution without shuffle like DataSet#coalesce
            String[] partitionColumns = config.getPartitionColumns();
            if (partitionColumns == null) {
                partitionColumns = info.schema().names();
            }

            Expression[] expressions = new Expression[partitionColumns.length];
            for (int i = 0; i < partitionColumns.length; i++) {
                expressions[i] = Expressions.column(partitionColumns[i]);
            }

            return Distributions.clustered(expressions);
        }

        @Override
        public SortOrder[] requiredOrdering() {
            return new SortOrder[0];
        }
    }
}
