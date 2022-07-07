package com.starrocks.connector.spark.sql.conf;

import java.util.Map;

public class ReadStarRocksConfig implements StarRocksConfig {

    public ReadStarRocksConfig(Map<String, String> properties) {

    }

    @Override
    public Map<String, String> getOriginOptions() {
        return null;
    }

    @Override
    public String getDatabase() {
        return get(READ_PREFIX + "database");
    }

    @Override
    public String getTable() {
        return get(READ_PREFIX + "table");
    }

    @Override
    public String[] getColumns() {
        return new String[0];
    }

    @Override
    public String getFeJdbcUrl() {
        return get(READ_PREFIX + "fe.urls.jdbc");
    }

    @Override
    public String getUsername() {
        return null;
    }

    @Override
    public String getPassword() {
        return null;
    }

    @Override
    public StarRocksConfig withOptions(Map<String, String> options) {
        return null;
    }
}
