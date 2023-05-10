package com.starrocks.connector.spark.sql.conf;

import com.starrocks.data.load.stream.StreamLoadDataFormat;
import com.starrocks.data.load.stream.properties.StreamLoadProperties;
import com.starrocks.data.load.stream.properties.StreamLoadTableProperties;

import java.io.Serializable;
import java.util.Map;
import java.util.stream.Collectors;

public class WriteStarRocksConfig implements StarRocksConfig, Serializable {

    private static final String KEY_FE_HTTP = WRITE_PREFIX + "fe.urls.http";
    private static final String KEY_FE_JDBC = WRITE_PREFIX + "fe.urls.jdbc";
    private static final String KEY_DATABASE = WRITE_PREFIX + "database";
    private static final String KEY_TABLE = WRITE_PREFIX + "table";
    private static final String KEY_COLUMNS = WRITE_PREFIX + "columns";
    private static final String KEY_USERNAME = WRITE_PREFIX + "username";
    private static final String KEY_PASSWORD = WRITE_PREFIX + "password";

    private static final String CTL_PREFIX = WRITE_PREFIX + "ctl.";
    private static final String KEY_CTL_ENABLE_TRANSACTION = CTL_PREFIX + "enable-transaction";
    private static final String KEY_CTL_CACHE_MAX_BYTES = CTL_PREFIX + "cacheMaxBytes";
    private static final String KEY_CTL_EXPECT_DELAY_TIME = CTL_PREFIX + "expectDelayTime";
    private static final String KEY_CTL_CONNECT_TIME_OUT = CTL_PREFIX + "connectTimeout";
    private static final String KEY_CTL_LABEL_PREFIX = CTL_PREFIX + "labelPrefix";
    private static final String KEY_CTL_IO_THREAD_COUNT = CTL_PREFIX + "ioThreadCount";
    private static final String KEY_CTL_CHUNK_LIMIT = CTL_PREFIX + "chunk_limit";

    private static final String PROPS_PREFIX = WRITE_PREFIX + "properties.";
    private static final String KEY_PROPS_FORMAT = PROPS_PREFIX + "format";
    private static final String KEY_PROPS_ROW_DELIMITER = PROPS_PREFIX + "row_delimiter";
    private static final String KEY_PROPS_COLUMN_SEPARATOR = PROPS_PREFIX + "column_separator";
    private final Map<String, String> originOptions;

    private String[] feHttpUrls;
    private String feJdbcUrl;
    private String username;
    private String password;
    private String database;
    private String table;
    private String[] columns;

    // ------------ CTL --------------- //
    private boolean enableTransaction;
    private Long cacheMaxBytes;
    private Long expectDelayTime;
    private Integer connectTimeout;
    private String labelPrefix;
    private Integer ioThreadCount;

    private Long chunkLimit;

    private Map<String, String> properties;

    public WriteStarRocksConfig(Map<String, String> originOptions) {
        this.originOptions = originOptions;
        load();
    }

    private void load() {
        feHttpUrls = getArray(KEY_FE_HTTP, new String[0]);
        feJdbcUrl = get(KEY_FE_JDBC);
        username = get(KEY_USERNAME);
        password = get(KEY_PASSWORD);
        database = get(KEY_DATABASE);
        table = get(KEY_TABLE);
        columns = getArray(KEY_COLUMNS, null);

        enableTransaction = getBoolean(KEY_CTL_ENABLE_TRANSACTION, false);
        cacheMaxBytes = getLong(KEY_CTL_CACHE_MAX_BYTES);
        expectDelayTime = getLong(KEY_CTL_EXPECT_DELAY_TIME);
        connectTimeout = getInt(KEY_CTL_CONNECT_TIME_OUT);
        labelPrefix = get(KEY_CTL_LABEL_PREFIX);
        ioThreadCount = getInt(KEY_CTL_IO_THREAD_COUNT, 1);

        chunkLimit = getLong(KEY_CTL_CHUNK_LIMIT, 3221225472L);

        properties = originOptions.entrySet().stream()
                .filter(entry -> entry.getKey().startsWith(PROPS_PREFIX))
                .collect(
                        Collectors.toMap(
                                entry -> entry.getKey().replaceFirst(PROPS_PREFIX, ""),
                                Map.Entry::getValue
                        )
                );

        if (!properties.containsKey("columns") && columns != null) {
            properties.put("columns", get(KEY_COLUMNS));
        }
        String format = getFormat();
        properties.put("format", format); // force insert format key for stream load http header
        if ("json".equalsIgnoreCase(format)) {
            properties.put("strip_outer_array", "true"); // stream loader sdk will surround json rows with []
        }
    }

    @Override
    public Map<String, String> getOriginOptions() {
        return originOptions;
    }

    @Override
    public WriteStarRocksConfig withOptions(Map<String, String> options) {
        return new WriteStarRocksConfig(withOverrides(WRITE_PREFIX, options));
    }

    @Override
    public String getDatabase() {
        return database;
    }

    @Override
    public String getTable() {
        return table;
    }

    @Override
    public String getUsername() {
        return username;
    }

    @Override
    public String getPassword() {
        return password;
    }

    @Override
    public String[] getColumns() {
        return columns;
    }

    public String[] getFeHttpUrls() {
        return feHttpUrls;
    }

    public String getFeJdbcUrl() {
        return feJdbcUrl;
    }

    public Long getCacheMaxBytes() {
        return cacheMaxBytes;
    }

    public Long getExpectDelayTime() {
        return expectDelayTime;
    }

    public Integer getConnectTimeout() {
        return connectTimeout;
    }

    public String getLabelPrefix() {
        return labelPrefix;
    }

    public int getIoThreadCount() {
        return ioThreadCount;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public String getFormat() {
        return originOptions.getOrDefault(KEY_PROPS_FORMAT, "json");
    }

    public String getRowDelimiter() {
        return originOptions.getOrDefault(KEY_PROPS_ROW_DELIMITER, "\n");
    }

    public String getColumnSeparator() {
        return originOptions.getOrDefault(KEY_PROPS_COLUMN_SEPARATOR, "\t");
    }

    public Long getChunkLimit() {
        return chunkLimit;
    }

    public StreamLoadProperties toStreamLoadProperties() {
        Map<String, String> props = getProperties();
        String format = getFormat();
        String rowDelimiter = getRowDelimiter();
        String joinedColumns = columns == null ? null : String.join(",", columns);
        StreamLoadTableProperties tableProperties = StreamLoadTableProperties.builder()
                .database(getDatabase())
                .table(getTable())
                .columns(joinedColumns)
                .streamLoadDataFormat("json".equalsIgnoreCase(format) ? StreamLoadDataFormat.JSON : new StreamLoadDataFormat.CSVFormat(rowDelimiter))
                .chunkLimit(chunkLimit)
                .addProperties(props)
                .build();

        StreamLoadProperties.Builder builder = StreamLoadProperties.builder()
                .defaultTableProperties(tableProperties)
                .jdbcUrl(getFeJdbcUrl())
                .loadUrls(getFeHttpUrls())
                .ioThreadCount(getIoThreadCount())
                .username(getUsername())
                .password(getPassword());

        if (enableTransaction) {
            builder.enableTransaction();
        }

        if (getCacheMaxBytes() != null) {
            builder.cacheMaxBytes(getCacheMaxBytes());
        }
        if (getExpectDelayTime() != null) {
            builder.expectDelayTime(getExpectDelayTime());
        }
        if (getConnectTimeout() != null) {
            builder.connectTimeout(getConnectTimeout());
        }
        if (getLabelPrefix() != null) {
            builder.labelPrefix(getLabelPrefix());
        }

        return builder.build();
    }
}
