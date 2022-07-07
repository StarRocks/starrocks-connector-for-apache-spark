package com.starrocks.connector.spark.sql.conf;

import java.util.HashMap;
import java.util.Map;

public class SimpleStarRocksConfig implements StarRocksConfig {

    private final Map<String, String> options;

    public SimpleStarRocksConfig(Map<String, String> options) {
        this.options = options;
    }

    @Override
    public Map<String, String> getOriginOptions() {
        return options;
    }

    @Override
    public String getDatabase() {
        return get(PREFIX + "database");
    }

    @Override
    public String getTable() {
        return get(PREFIX + "table");
    }

    @Override
    public String[] getColumns() {
        return new String[0];
    }

    @Override
    public String getFeJdbcUrl() {
        return get(PREFIX + "fe.urls.jdbc");
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
        Map<String, String> newOptions = new HashMap<>(options);
        newOptions.putAll(options);
        return new SimpleStarRocksConfig(newOptions);
    }
}
