package com.starrocks.connector.spark.sql.conf;

import scala.Serializable;

import java.util.Map;

public class ReadStarRocksConfig implements StarRocksConfig, Serializable {

    private static final String KEY_REQUEST_PREFIX = READ_PREFIX + "request.";

    private static final String KEY_REQUEST_QUERY_TIMEOUT = KEY_REQUEST_PREFIX + "query-timeout-ms";
    private static final String KEY_REQUEST_TABLET_SIZE = KEY_REQUEST_PREFIX + "tablet-size";

    private static final String KEY_QUERY_PREFIX = READ_PREFIX + "query.";
    private static final String KEY_QUERY_BATCH_SIZE = KEY_QUERY_PREFIX + "batch-size";
    private static final String KEY_QUERY_MEM_LIMIT_BYTES = KEY_QUERY_PREFIX + "memory-limit-bytes";
    private static final String KEY_QUERY_DESERIALIZE_ARROW_ASYNC = KEY_QUERY_PREFIX + "deserialize-arrow-async";
    private static final String KEY_QUERY_DESERIALIZE_QUEUE_SIZE = KEY_QUERY_PREFIX + "deserialize-queue-size";

    private static final String KEY_QUERY_FILTER_PREFIX = KEY_QUERY_PREFIX + "filter.";
    private static final String KEY_QUERY_FILTER_IN_MAX_COUNT = KEY_QUERY_FILTER_PREFIX + "in-max-count";
    private static final String KEY_QUERY_FILTER_COND = KEY_QUERY_FILTER_PREFIX + "conditions";

    private final Map<String, String> properties;

    private StarRocksConfig parent;

    private String database;
    private String table;
    private String[] columns;
    private String username;
    private String password;
    private String[] feHttpUrls;
    private int requestRetries;
    private int requestConnectTimeoutMs;
    private int requestSocketTimeoutMs;

    // ---Request--- //
    private int requestQueryTimeoutMs;
    private int requestTabletSize;

    // ---Query--- //
    private int queryBatchSize;
    private long queryMemoryLimitBytes;
    private boolean queryDeserializeArrowAsync;
    private int queryDeserializeQueueSize;

    private String queryFilterConditions;
    private int queryFilterInMaxCount;

    public ReadStarRocksConfig(Map<String, String> properties) {
        this.properties = properties;
        load();
    }

    public ReadStarRocksConfig(StarRocksConfig parent) {
        this(parent.getOriginOptions());
        this.parent = parent;
    }

    private void load() {
        this.database = get(KEY_DATABASE);
        this.table = get(KEY_TABLE);
        this.columns = getArray(KEY_COLUMNS, null);
        this.username = get(KEY_USERNAME);
        this.password = get(KEY_PASSWORD);
        this.feHttpUrls = getArray(KEY_FE_HTTP, new String[0]);
        this.requestRetries = getInt(KEY_REQUEST_RETRIES, 3);
        this.requestConnectTimeoutMs = getInt(KEY_REQUEST_CONNECT_TIMEOUT, 30000);
        this.requestSocketTimeoutMs = getInt(KEY_REQUEST_SOCKET_TIMEOUT, 30000);

        this.requestQueryTimeoutMs = getInt(KEY_REQUEST_QUERY_TIMEOUT, 3600000);
        this.requestTabletSize = getInt(KEY_REQUEST_TABLET_SIZE, Integer.MAX_VALUE);

        this.queryBatchSize = getInt(KEY_QUERY_BATCH_SIZE, 1024);
        this.queryMemoryLimitBytes = getLong(KEY_QUERY_MEM_LIMIT_BYTES, 2147483648L);
        this.queryDeserializeArrowAsync = getBoolean(KEY_QUERY_DESERIALIZE_ARROW_ASYNC, false);
        this.queryDeserializeQueueSize = getInt(KEY_QUERY_DESERIALIZE_QUEUE_SIZE, 64);

        this.queryFilterConditions = get(KEY_QUERY_FILTER_COND);
        this.queryFilterInMaxCount = Math.min(getInt(KEY_QUERY_FILTER_IN_MAX_COUNT, 100), 10000);
    }

    @Override
    public Map<String, String> getOriginOptions() {
        return properties;
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
    public String[] getColumns() {
        return columns;
    }

    @Override
    public String getFeJdbcUrl() {
        return null;
    }

    @Override
    public String[] getFeHttpUrls() {
        return feHttpUrls;
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
    public int getRequestRetries() {
        return requestRetries;
    }

    @Override
    public int getRequestConnectTimeoutMs() {
        return requestConnectTimeoutMs;
    }

    @Override
    public int getRequestSocketTimeoutMs() {
        return requestSocketTimeoutMs;
    }

    @Override
    public StarRocksConfig withOptions(Map<String, String> options) {
        return new ReadStarRocksConfig(withOverrides(READ_PREFIX, options));
    }

    @Override
    public ReadStarRocksConfig toReadConfig() {
        return this;
    }

    @Override
    public WriteStarRocksConfig toWriteConfig() {
        if (parent != null) {
            return parent.toWriteConfig();
        }
        return StarRocksConfig.writeConfig(getOriginOptions());
    }


    public int getRequestQueryTimeoutMs() {
        return requestQueryTimeoutMs;
    }

    public int getRequestTabletSize() {
        return requestTabletSize;
    }

    public int getQueryBatchSize() {
        return queryBatchSize;
    }

    public long getQueryMemoryLimitBytes() {
        return queryMemoryLimitBytes;
    }

    public boolean isQueryDeserializeArrowAsync() {
        return queryDeserializeArrowAsync;
    }

    public int getQueryDeserializeQueueSize() {
        return queryDeserializeQueueSize;
    }

    public String getQueryFilterConditions() {
        return queryFilterConditions;
    }

    public int getQueryFilterInMaxCount() {
        return queryFilterInMaxCount;
    }
}
