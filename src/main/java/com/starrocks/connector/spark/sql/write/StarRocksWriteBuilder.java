package com.starrocks.connector.spark.sql.write;

import com.starrocks.connector.spark.sql.conf.WriteStarRocksConfig;
import com.starrocks.connector.spark.sql.schema.CsvRowStringConverter;
import com.starrocks.connector.spark.sql.schema.JSONRowStringConverter;
import com.starrocks.connector.spark.sql.schema.RowStringConverter;
import org.apache.spark.sql.connector.distributions.Distribution;
import org.apache.spark.sql.connector.distributions.Distributions;
import org.apache.spark.sql.connector.expressions.Expression;
import org.apache.spark.sql.connector.expressions.Expressions;
import org.apache.spark.sql.connector.expressions.SortOrder;
import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.RequiresDistributionAndOrdering;
import org.apache.spark.sql.connector.write.Write;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.connector.write.streaming.StreamingWrite;

public class StarRocksWriteBuilder implements WriteBuilder {
    private final LogicalWriteInfo info;
    private final WriteStarRocksConfig config;

    public StarRocksWriteBuilder(LogicalWriteInfo info, WriteStarRocksConfig config) {
        this.info = info;
        this.config = config;
    }

    @Override
    public Write build() {
        RowStringConverter converter;
        if ("csv".equalsIgnoreCase(config.getFormat())) {
            converter = new CsvRowStringConverter(info.schema(), config.getColumnSeparator());
        }  else if ("json".equalsIgnoreCase(config.getFormat())) {
            converter = new JSONRowStringConverter(info.schema());
        } else {
            throw new RuntimeException("UnSupport format " + config.getFormat());
        }
        return new StarRocksWriteImpl(info, config, converter);
    }

    private static class StarRocksWriteImpl implements Write, RequiresDistributionAndOrdering {

        private final LogicalWriteInfo info;
        private final WriteStarRocksConfig config;
        private final RowStringConverter converter;

        public StarRocksWriteImpl(LogicalWriteInfo info, WriteStarRocksConfig config, RowStringConverter converter) {
            this.info = info;
            this.config = config;
            this.converter = converter;
        }

        @Override
        public String description() {
            return String.format("StarRocksWriteImpl[%s.%s]", config.getDatabase(), config.getTable());
        }

        @Override
        public BatchWrite toBatch() {
            return new StarRocksWrite(info, config, converter);
        }

        @Override
        public StreamingWrite toStreaming() {
            return new StarRocksWrite(info, config, converter);
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
