package com.starrocks.connector.spark.sql;

import com.starrocks.connector.spark.sql.conf.StarRocksConfig;
import com.starrocks.connector.spark.sql.conf.WriteStarRocksConfig;
import com.starrocks.connector.spark.sql.write.StarRocksWriteBuilder;
import org.apache.spark.sql.connector.catalog.SupportsWrite;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class StarRocksTable implements Table, SupportsWrite {

    private static final Set<TableCapability> TABLE_CAPABILITY_SET = Collections.unmodifiableSet(
            new HashSet<>(
                    Arrays.asList(
                            TableCapability.BATCH_WRITE,
                            TableCapability.STREAMING_WRITE
                    )
            )
    );

    private final StructType schema;
    private final Transform[] partitioning;
    private final StarRocksConfig config;

    public StarRocksTable(StructType schema, StarRocksConfig config) {
        this(schema, null, config);
    }

    public StarRocksTable(StructType schema, Transform[] partitioning, StarRocksConfig config) {
        this.schema = schema;
        this.partitioning = partitioning;
        this.config = config;
    }

    @Override
    public WriteBuilder newWriteBuilder(LogicalWriteInfo info) {
        return new StarRocksWriteBuilder(info, new WriteStarRocksConfig(config.getOriginOptions()));
    }

    @Override
    public String name() {
        return String.format("StarRocksTable[%s.%s]", config.getDatabase(), config.getTable());
    }

    @Override
    public StructType schema() {
        return schema;
    }

    @Override
    public Set<TableCapability> capabilities() {
        return TABLE_CAPABILITY_SET;
    }
}
