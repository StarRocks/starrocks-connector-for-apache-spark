package com.starrocks.connector.spark.sql.conf;

import scala.Serializable;

import java.util.HashMap;
import java.util.Map;

public class SimpleStarRocksConfig implements StarRocksConfig, Serializable {

    private final Map<String, String> options;

    private String[] feHttpUrls;
    private String feJdbcUrl;
    private String username;
    private String password;
    private String database;
    private String table;
    private String[] columns;
    private int requestRetries;
    private int requestConnectTimeoutMs;
    private int requestSocketTimeoutMs;

    private final ReadStarRocksConfig readConf;
    private final WriteStarRocksConfig writeConf;

    public SimpleStarRocksConfig(Map<String, String> options) {
        this.options = options;
        this.readConf = StarRocksConfig.readConfig(this);
        this.writeConf = StarRocksConfig.writeConfig(this);
        load();
    }

    private void load() {
        this.feHttpUrls = getArray(KEY_FE_HTTP, new String[0]);
        this.feJdbcUrl = get(KEY_FE_JDBC);
        this.username = get(KEY_USERNAME);
        this.password = get(KEY_PASSWORD);
        this.database = get(KEY_DATABASE);
        this.table = get(KEY_TABLE);
        this.columns = getArray(KEY_COLUMNS, null);
        this.requestRetries = getInt(KEY_REQUEST_RETRIES, 3);
        this.requestConnectTimeoutMs = getInt(KEY_REQUEST_CONNECT_TIMEOUT, 30000);
        this.requestSocketTimeoutMs = getInt(KEY_REQUEST_SOCKET_TIMEOUT, 30000);


    }

    @Override
    public Map<String, String> getOriginOptions() {
        return options;
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
        return feJdbcUrl;
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
        Map<String, String> newOptions = new HashMap<>(options);
        newOptions.putAll(options);
        return new SimpleStarRocksConfig(newOptions);
    }

    @Override
    public ReadStarRocksConfig toReadConfig() {
        return readConf;
    }

    @Override
    public WriteStarRocksConfig toWriteConfig() {
        return writeConf;
    }
}
