package com.starrocks.connector.spark.sql.conf;

import com.starrocks.data.load.stream.StreamLoadDataFormat;
import com.starrocks.data.load.stream.StreamLoadUtils;
import com.starrocks.data.load.stream.properties.StreamLoadProperties;
import com.starrocks.data.load.stream.properties.StreamLoadTableProperties;
import org.apache.spark.util.Utils;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

public class WriteStarRocksConfig extends StarRocksConfigBase {

    private static final long serialVersionUID = 1L;

    private static final String WRITE_PREFIX = PREFIX + "write.";
    // The prefix of the stream load label. Available values are within [-_A-Za-z0-9]
    private static final String KEY_LABEL_PREFIX = WRITE_PREFIX + "label.prefix";
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
    // Flush interval of the row batch in millisecond
    private static final String KEY_FLUSH_INTERVAL = WRITE_PREFIX + "flush.interval.ms";
    private static final String PROPS_PREFIX = WRITE_PREFIX + "properties.";
    private static final String KEY_PROPS_FORMAT = PROPS_PREFIX + "format";
    private static final String KEY_PROPS_ROW_DELIMITER = PROPS_PREFIX + "row_delimiter";
    private static final String KEY_PROPS_COLUMN_SEPARATOR = PROPS_PREFIX + "column_separator";

    private static final String KEY_NUM_PARTITIONS = WRITE_PREFIX + "num.partitions";
    private static final String KEY_PARTITION_COLUMNS = WRITE_PREFIX + "partition.columns";

    private String labelPrefix = "spark-";
    private int waitForContinueTimeoutMs = 30000;
    // Only support to write to one table, and one thread is enough
    private int ioThreadCount = 1;
    private long chunkLimit = 3221225472L;
    private int scanFrequencyInMs = 50;
    private boolean enableTransactionStreamLoad = true;
    private long bufferSize = 104857600;
    private int flushInterval = 300000;
    private Map<String, String> properties;
    private String format = "CSV";
    private String rowDelimiter = "\n";
    private String columnSeparator = "\t";
    private boolean supportTransactionStreamLoad = true;

    // According to Spark RequiresDistributionAndOrdering#requiredNumPartitions(),
    // any value less than 1 mean no requirement
    private int numPartitions = 0;
    // columns used for partition. will use all columns if not set
    private String[] partitionColumns;

    public WriteStarRocksConfig(Map<String, String> originOptions) {
        super(originOptions);
        load();
    }

    private void load() {
        labelPrefix = get(KEY_LABEL_PREFIX);
        waitForContinueTimeoutMs = getInt(KEY_WAIT_FOR_CONTINUE_TIMEOUT, 30000);
        chunkLimit = Utils.byteStringAsBytes(get(KEY_CHUNK_LIMIT, "3g"));
        scanFrequencyInMs = getInt(KEY_SCAN_FREQUENCY, 50);
        enableTransactionStreamLoad = getBoolean(KEY_ENABLE_TRANSACTION, true);
        bufferSize = Utils.byteStringAsBytes(get(KEY_BUFFER_SIZE, "100m"));
        flushInterval = getInt(KEY_FLUSH_INTERVAL, 300000);

        properties = originOptions.entrySet().stream()
                .filter(entry -> entry.getKey().startsWith(PROPS_PREFIX))
                .collect(
                        Collectors.toMap(
                                entry -> entry.getKey().replaceFirst(PROPS_PREFIX, ""),
                                Map.Entry::getValue
                        )
                );
        format = originOptions.getOrDefault(KEY_PROPS_FORMAT, "CSV");
        rowDelimiter = originOptions.getOrDefault(KEY_PROPS_ROW_DELIMITER, "\n");
        columnSeparator = originOptions.getOrDefault(KEY_PROPS_COLUMN_SEPARATOR, "\t");
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

    public StreamLoadProperties toStreamLoadProperties() {
        StreamLoadDataFormat dataFormat = "json".equalsIgnoreCase(format) ?
                StreamLoadDataFormat.JSON : new StreamLoadDataFormat.CSVFormat(rowDelimiter);
        String columns = null;
        if (!properties.containsKey("columns") && getColumns() != null) {
            columns = Arrays.stream(getColumns())
                    .map(f -> String.format("`%s`", f.trim().replace("`", "")))
                    .collect(Collectors.joining(","));
        }
        StreamLoadTableProperties tableProperties = StreamLoadTableProperties.builder()
                .database(getDatabase())
                .table(getTable())
                .columns(columns)
                .streamLoadDataFormat(dataFormat)
                .chunkLimit(chunkLimit)
                .columns(columns)
                .build();

        StreamLoadProperties.Builder builder = StreamLoadProperties.builder()
                .defaultTableProperties(tableProperties)
                .loadUrls(getFeHttpUrls())
                .jdbcUrl(getFeJdbcUrl())
                .username(getUsername())
                .password(getPassword())
                .connectTimeout(getHttpRequestConnectTimeoutMs())
                .waitForContinueTimeoutMs(waitForContinueTimeoutMs)
                .ioThreadCount(ioThreadCount)
                .scanningFrequency(scanFrequencyInMs)
                .cacheMaxBytes(bufferSize)
                .expectDelayTime(flushInterval)
                .labelPrefix(labelPrefix)
                .addHeaders(properties);

        if (enableTransactionStreamLoad && supportTransactionStreamLoad) {
            builder.enableTransaction();
        }

        return builder.build();
    }
}
