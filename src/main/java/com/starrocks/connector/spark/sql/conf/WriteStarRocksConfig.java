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

package com.starrocks.connector.spark.sql.conf;

import com.starrocks.connector.spark.sql.schema.StarRocksField;
import com.starrocks.connector.spark.sql.schema.StarRocksSchema;
import com.starrocks.data.load.stream.DelimiterParser;
import com.starrocks.data.load.stream.StreamLoadDataFormat;
import com.starrocks.data.load.stream.StreamLoadUtils;
import com.starrocks.data.load.stream.properties.StreamLoadProperties;
import com.starrocks.data.load.stream.properties.StreamLoadTableProperties;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.ByteType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.IntegerType;
import org.apache.spark.sql.types.LongType;
import org.apache.spark.sql.types.ShortType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.util.Utils;

import java.util.*;
import java.util.stream.Collectors;

public class WriteStarRocksConfig extends StarRocksConfigBase {

    private static final long serialVersionUID = 1L;

    public static final String WRITE_PREFIX = PREFIX + "write.";
    // The prefix of the stream load label. Available values are within [-_A-Za-z0-9]
    private static final String KEY_LABEL_PREFIX = WRITE_PREFIX + "label.prefix";
    private static final String KEY_SOCKET_TIMEOUT = WRITE_PREFIX + "socket.timeout.ms";
    // Timeout in millisecond to wait for 100-continue response from FE
    private static final String KEY_WAIT_FOR_CONTINUE_TIMEOUT = WRITE_PREFIX + "wait-for-continue.timeout.ms";
    // Data chunk size in a http request for stream load
    private static final String KEY_CHUNK_LIMIT = WRITE_PREFIX + "chunk.limit";
    // Scan frequency in milliseconds
    private static final String KEY_SCAN_FREQUENCY = WRITE_PREFIX + "scan-frequency.ms";
    // Whether to use transaction stream load
    private static final String KEY_ENABLE_TRANSACTION = WRITE_PREFIX + "enable.transaction-stream-load";
    // The memory size used to buffer the rows before loading the data to StarRocks.
    // This can improve the performance for writing to starrocks.
    private static final String KEY_BUFFER_SIZE = WRITE_PREFIX + "buffer.size";
    // The number of rows buffered before sending to StarRocks.
    private static final String KEY_BUFFER_ROWS = WRITE_PREFIX + "buffer.rows";

    // Flush interval of the row batch in millisecond
    private static final String KEY_FLUSH_INTERVAL = WRITE_PREFIX + "flush.interval.ms";
    private static final String KEY_MAX_RETIES = WRITE_PREFIX + "max.retries";
    private static final String KEY_RETRY_INTERVAL_MS = WRITE_PREFIX + "retry.interval.ms";
    private static final String PROPS_PREFIX = WRITE_PREFIX + "properties.";
    private static final String KEY_PROPS_FORMAT = PROPS_PREFIX + "format";
    private static final String KEY_PROPS_ROW_DELIMITER = PROPS_PREFIX + "row_delimiter";
    private static final String KEY_PROPS_COLUMN_SEPARATOR = PROPS_PREFIX + "column_separator";

    private static final String KEY_NUM_PARTITIONS = WRITE_PREFIX + "num.partitions";
    private static final String KEY_PARTITION_COLUMNS = WRITE_PREFIX + "partition.columns";
    // start use stage mode or not, if not stage mode will never use
    //decide stageMode when to use  always,auto,never
    private static final String STAGE_MODE_USE = WRITE_PREFIX + "stage.use";
    // stage config header
    private static final String STAGE_CONFIG = WRITE_PREFIX + "stage.";

    public enum StageUse {
        ALWAYS,
        AUTO,
        NEVER
    }

    private String labelPrefix = "spark";
    private int socketTimeoutMs = -1;
    private int waitForContinueTimeoutMs = 30000;
    // Only support to write to one table, and one thread is enough
    private int ioThreadCount = 1;
    private long chunkLimit = 3221225472L;
    private int scanFrequencyInMs = 50;
    private boolean enableTransactionStreamLoad = true;
    private long bufferSize = 104857600;
    private int bufferRows = Integer.MAX_VALUE;
    private int flushInterval = 300000;
    private int maxRetries = 0;
    private int retryIntervalInMs = 10000;
    private Map<String, String> properties;
    private String format = "CSV";
    private String rowDelimiter = "\n";
    private String columnSeparator = "\t";
    private boolean supportTransactionStreamLoad = true;
    private StageUse stageUse = StageUse.NEVER;
    private Map<String, String> stageConfig;

    //starrocks.write.stage.columns.update.ratio
    // a rough number describe the ratio of row ro be updated 1-100

    //if any column in stageUpdateColumn to be updated use stage mode default ""
    // According to Spark RequiresDistributionAndOrdering#requiredNumPartitions(),
    // any value less than 1 mean no requirement
    private int numPartitions = 0;
    // columns used for partition. will use all columns if not set
    private String[] partitionColumns;

    private String streamLoadColumnProperty;
    private String[] streamLoadColumnNames;
    private  Set<String> starRocksJsonColumnNames;
    private StructType sparkSchema;

    public StructType getSparkSchema() {
        return sparkSchema;
    }

    public WriteStarRocksConfig(Map<String, String> originOptions, StructType sparkSchema, StarRocksSchema starRocksSchema) {
        super(originOptions);
        load(sparkSchema);
        this.sparkSchema = sparkSchema;
        genStreamLoadColumns(sparkSchema, starRocksSchema);
        this.starRocksJsonColumnNames = new HashSet<>();
        for (StarRocksField column : starRocksSchema.getColumns()) {
            if (column.isJson()) {
                starRocksJsonColumnNames.add(column.getName());
            }
        }
    }

    private void load(StructType sparkSchema) {
        labelPrefix = get(KEY_LABEL_PREFIX, "spark");
        socketTimeoutMs = getInt(KEY_SOCKET_TIMEOUT, -1);
        waitForContinueTimeoutMs = getInt(KEY_WAIT_FOR_CONTINUE_TIMEOUT, 30000);
        chunkLimit = Utils.byteStringAsBytes(get(KEY_CHUNK_LIMIT, "3g"));
        scanFrequencyInMs = getInt(KEY_SCAN_FREQUENCY, 50);
        enableTransactionStreamLoad = getBoolean(KEY_ENABLE_TRANSACTION, true);
        bufferSize = Utils.byteStringAsBytes(get(KEY_BUFFER_SIZE, "100m"));
        bufferRows = getInt(KEY_BUFFER_ROWS, Integer.MAX_VALUE);
        flushInterval = getInt(KEY_FLUSH_INTERVAL, 300000);
        maxRetries = getInt(KEY_MAX_RETIES, 3);
        retryIntervalInMs = getInt(KEY_RETRY_INTERVAL_MS, 10000);
        stageConfig= originOptions.entrySet().stream()
                .filter(entry -> entry.getKey().startsWith(STAGE_CONFIG))
                .collect(
                        Collectors.toMap(
                                entry -> entry.getKey().replaceFirst(STAGE_CONFIG, ""),
                                Map.Entry::getValue
                        )
                );

        stageUse =StageUse.valueOf(get(STAGE_MODE_USE, "never").toUpperCase());

        properties = originOptions.entrySet().stream()
                .filter(entry -> entry.getKey().startsWith(PROPS_PREFIX))
                .collect(
                        Collectors.toMap(
                                entry -> entry.getKey().replaceFirst(PROPS_PREFIX, ""),
                                Map.Entry::getValue
                        )
                );
        format = originOptions.getOrDefault(KEY_PROPS_FORMAT, "CSV");
        rowDelimiter = DelimiterParser.convertDelimiter(
                originOptions.getOrDefault(KEY_PROPS_ROW_DELIMITER, "\n"));
        columnSeparator = DelimiterParser.convertDelimiter(
                originOptions.getOrDefault(KEY_PROPS_COLUMN_SEPARATOR, "\t"));
        String inferedFormat = inferFormatFromSchema(sparkSchema);
        if (inferedFormat != null) {
            format = inferedFormat;
            properties.put("format", format);
        }
        if ("json".equalsIgnoreCase(format)) {
            if (!properties.containsKey("strip_outer_array")) {
                properties.put("strip_outer_array", "true");
            }

            if (!properties.containsKey("ignore_json_size")) {
                properties.put("ignore_json_size", "true");
            }
        }
        if (!properties.containsKey("timeout")) {
            int timeout = Math.max(600, flushInterval / 1000 + 600);
            properties.put("timeout", String.valueOf(timeout));
        }

        numPartitions = getInt(KEY_NUM_PARTITIONS, 0);
        partitionColumns = getArray(KEY_PARTITION_COLUMNS, null);
        supportTransactionStreamLoad = StreamLoadUtils.isStarRocksSupportTransactionLoad(
                Arrays.asList(getFeHttpUrls()), getHttpRequestConnectTimeoutMs(), getUsername(), getPassword());
    }
    private WriteStarRocksConfig() {
        super();
    }
    public WriteStarRocksConfig copy(String dataBase, String table, List<String> excludeStreamLoadProperties) {
        WriteStarRocksConfig copyConfig = new WriteStarRocksConfig();

        super.copy(copyConfig);
        copyConfig.database = dataBase;
        copyConfig.table = table;
        copyConfig.labelPrefix = labelPrefix;
        copyConfig.waitForContinueTimeoutMs = waitForContinueTimeoutMs;
        copyConfig.ioThreadCount = ioThreadCount;
        copyConfig.chunkLimit = chunkLimit;
        copyConfig.scanFrequencyInMs = scanFrequencyInMs;
        copyConfig.enableTransactionStreamLoad = enableTransactionStreamLoad;
        copyConfig.bufferSize = bufferSize;
        copyConfig.bufferRows = bufferRows;
        copyConfig.flushInterval = flushInterval;
        copyConfig.maxRetries = maxRetries;
        copyConfig.retryIntervalInMs = retryIntervalInMs;
        copyConfig.properties = new HashMap<>(properties);
        excludeStreamLoadProperties.forEach(copyConfig.properties::remove);
        copyConfig.format = format;
        copyConfig.rowDelimiter = rowDelimiter;
        copyConfig.columnSeparator = columnSeparator;
        copyConfig.supportTransactionStreamLoad = supportTransactionStreamLoad;
        copyConfig.numPartitions = numPartitions;
        copyConfig.partitionColumns = partitionColumns;
        copyConfig.streamLoadColumnProperty = streamLoadColumnProperty;
        copyConfig.streamLoadColumnNames = streamLoadColumnNames;

        return copyConfig;
    }

    private void genStreamLoadColumns(StructType sparkSchema, StarRocksSchema starRocksSchema) {
        streamLoadColumnNames = new String[sparkSchema.length()];
        List<String> expressions = new ArrayList<>();
        for (int i = 0; i < sparkSchema.length(); i++) {
            StructField field = sparkSchema.apply(i);
            StarRocksField starRocksField = starRocksSchema.getField(field.name());
            if (starRocksField.isBitmap()) {
                streamLoadColumnNames[i] = "__tmp" + field.name();
                expressions.add(String.format("`%s`=%s(`%s`)",
                        field.name(), getBitmapFunction(field), streamLoadColumnNames[i]));
            } else if (starRocksField.isHll()) {
                streamLoadColumnNames[i] = "__tmp" + field.name();
                expressions.add(String.format("`%s`=hll_hash(`%s`)", field.name(), streamLoadColumnNames[i]));
            } else {
                streamLoadColumnNames[i] = field.name();
            }
        }

        if (properties.containsKey("columns")) {
            streamLoadColumnProperty = properties.get("columns");
        } else if (getColumns() != null || !expressions.isEmpty()) {
            String joinedCols = Arrays.stream(streamLoadColumnNames)
                    .map(f -> String.format("`%s`", f.trim().replace("`", "")))
                    .collect(Collectors.joining(","));
            String joinedExps = String.join(",", expressions);
            streamLoadColumnProperty = joinedExps.isEmpty() ? joinedCols : joinedCols + "," + joinedExps;
        }
    }

    // Infer the format used by stream load from the spark schema.
    // Returns null if can't infer the format
    private String inferFormatFromSchema(StructType sparkSchema) {
        for (StructField field : sparkSchema.fields()) {
            // TODO there is no standard about how to represent array type in csv format,
            //  so force to use json format if there is array type
            if (field.dataType() instanceof ArrayType) {
                return "json";
            }
        }
        return null;
    }

    private String getBitmapFunction(StructField field) {
        DataType dataType = field.dataType();
        if (dataType instanceof ByteType
            || dataType instanceof ShortType
            || dataType instanceof IntegerType
            || dataType instanceof LongType) {
            return "to_bitmap";
        } else {
            return "bitmap_hash";
        }
    }


    public StageUse getStageUse() {
        return stageUse;
    }


    public Map<String, String> getStageConfig() {
        return stageConfig;
    }
    public String getFormat() {
        return format;
    }

    public String getColumnSeparator() {
        return columnSeparator;
    }

    public int getNumPartitions() {
        return numPartitions;
    }

    public String[] getPartitionColumns() {
        return partitionColumns;
    }

    public String[] getStreamLoadColumnNames() {
        return streamLoadColumnNames;
    }

    public Set<String> getStarRocksJsonColumnNames() {
        return starRocksJsonColumnNames;
    }





    public boolean isPartialUpdate() {
        String val = properties.get("partial_update");
        return val != null && val.equalsIgnoreCase("true");
    }
    public boolean isPartialUpdateColumnMode() {
        String val = properties.get("partial_update_mode");
        return val != null && val.equalsIgnoreCase("column");
    }
    public StreamLoadProperties toStreamLoadProperties() {
        StreamLoadDataFormat dataFormat = "json".equalsIgnoreCase(format) ?
                StreamLoadDataFormat.JSON : new StreamLoadDataFormat.CSVFormat(rowDelimiter);

        StreamLoadTableProperties tableProperties = StreamLoadTableProperties.builder()
                .database(getDatabase())
                .table(getTable())
                .columns(streamLoadColumnProperty)
                .streamLoadDataFormat(dataFormat)
                .chunkLimit(chunkLimit)
                .maxBufferRows(bufferRows)
                .addCommonProperties(properties)
                .build();

        StreamLoadProperties.Builder builder = StreamLoadProperties.builder()
                .defaultTableProperties(tableProperties)
                .loadUrls(getFeHttpUrls())
                .jdbcUrl(getFeJdbcUrl())
                .username(getUsername())
                .password(getPassword())
                .connectTimeout(getHttpRequestConnectTimeoutMs())
                .socketTimeout(socketTimeoutMs)
                .waitForContinueTimeoutMs(waitForContinueTimeoutMs)
                .ioThreadCount(ioThreadCount)
                .scanningFrequency(scanFrequencyInMs)
                .cacheMaxBytes(bufferSize)
                .expectDelayTime(flushInterval)
                .labelPrefix(labelPrefix)
                .maxRetries(maxRetries)
                .retryIntervalInMs(retryIntervalInMs)
                .addHeaders(properties);

        if (enableTransactionStreamLoad && supportTransactionStreamLoad) {
            builder.enableTransaction();
        }

        return builder.build();
    }
}
