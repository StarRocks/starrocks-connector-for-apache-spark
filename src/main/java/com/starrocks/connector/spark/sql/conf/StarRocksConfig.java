package com.starrocks.connector.spark.sql.conf;

import com.starrocks.connector.spark.sql.schema.StarRocksField;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public interface StarRocksConfig {

    static StarRocksConfig createConfig(final Map<String, String> options) {
        return new SimpleStarRocksConfig(options);
    }

    static ReadStarRocksConfig readConfig(final Map<String, String> options) {
        return new ReadStarRocksConfig(options);
    }

    static ReadStarRocksConfig readConfig(final StarRocksConfig parent) {
        return new ReadStarRocksConfig(parent);
    }

    static WriteStarRocksConfig writeConfig(final Map<String, String> options) {
        return new WriteStarRocksConfig(options);
    }

    static WriteStarRocksConfig writeConfig(final StarRocksConfig parent) {
        return new WriteStarRocksConfig(parent);
    }

    String PREFIX = "spark.starrocks.";
    String WRITE_PREFIX = PREFIX + "write.";
    String READ_PREFIX = PREFIX + "read.";
    String INFER_PREFIX = PREFIX + "infer.";

    String KEY_FE_HTTP = PREFIX + "fe.urls.http";
    String KEY_FE_JDBC = PREFIX + "fe.urls.jdbc";
    String KEY_DATABASE = PREFIX + "database";
    String KEY_TABLE = PREFIX + "table";
    String KEY_COLUMNS = PREFIX + "columns";
    String KEY_USERNAME = PREFIX + "username";
    String KEY_PASSWORD = PREFIX + "password";

    String KEY_REQUEST_RETRIES = PREFIX + "retries";
    String KEY_REQUEST_CONNECT_TIMEOUT = PREFIX + "connect-timeout-ms";
    String KEY_REQUEST_SOCKET_TIMEOUT = PREFIX + "socket-timeout-ms";

    String CONF_TYPE = PREFIX + "conf";

    interface InferConf {
        String KEY_COLUMNS = INFER_PREFIX + "columns";
        String KEY_COLUMN_PREFIX = INFER_PREFIX + "column.";
        String KEY_COLUMN_TYPE_SUFFIX = ".type";
        String KEY_COLUMN_PRECISION_SUFFIX = ".precision";
        String KEY_COLUMN_SCALE_SUFFIX = ".scale";
    }

    Map<String, String> getOriginOptions();

    String getDatabase();

    String getTable();

    String[] getColumns();

    String getFeJdbcUrl();

    String[] getFeHttpUrls();

    String getUsername();

    String getPassword();

    int getRequestRetries();

    int getRequestConnectTimeoutMs();

    int getRequestSocketTimeoutMs();

    StarRocksConfig withOptions(Map<String, String> options);

    default List<StarRocksField> inferFields() {
        Map<String, String> options = getOriginOptions();

        String inferColumns = options.get(InferConf.KEY_COLUMNS);
        if (inferColumns == null) {
            return Collections.emptyList();
        }

        String[] columns = inferColumns.split(",");
        List<StarRocksField> fields = new ArrayList<>(columns.length);
        for (String column : columns) {
            StarRocksField field = new StarRocksField();
            field.setName(column);
            field.setType(options.get(InferConf.KEY_COLUMN_PREFIX + column + InferConf.KEY_COLUMN_TYPE_SUFFIX));
            field.setSize(options.get(InferConf.KEY_COLUMN_PREFIX + column + InferConf.KEY_COLUMN_PRECISION_SUFFIX));
            field.setScale(options.get(InferConf.KEY_COLUMN_PREFIX + column + InferConf.KEY_COLUMN_SCALE_SUFFIX));
            fields.add(field);
        }
        return fields;
    }

    default Map<String, String> withOverrides(String prefix, Map<String, String> options) {
        Map<String, String> newOptions = new HashMap<>(getOriginOptions());

        options.forEach(
                (k, v) -> {
                    if (k.startsWith(prefix)) {
                        newOptions.put(k, v);
                    }
                }
        );

        return newOptions;
    }

    default WriteStarRocksConfig toWriteConfig() {
        if (this instanceof WriteStarRocksConfig) {
            return (WriteStarRocksConfig) this;
        }
        return writeConfig(getOriginOptions());
    }

    default ReadStarRocksConfig toReadConfig() {
        if (this instanceof ReadStarRocksConfig) {
            return (ReadStarRocksConfig) this;
        }
        return readConfig(getOriginOptions());
    }

    default String get(final String key) {
        return getOriginOptions().get(key);
    }

    default String getOrDefault(final String key, final String defaultValue) {
        String value = get(key);
        return value == null ? defaultValue : value;
    }

    default boolean getBoolean(final String key, final boolean defaultValue) {
        String value = get(key);
        if (value == null) {
            return defaultValue;
        } else if (value.equalsIgnoreCase("true")) {
            return true;
        } else if (value.equalsIgnoreCase("false")) {
            return false;
        } else {
            throw new RuntimeException(value + " is not a boolean string");
        }
    }

    default int getInt(final String key, final int defaultValue) {
        String value = get(key);
        return value == null ? defaultValue : Integer.parseInt(get(key));
    }

    default Integer getInt(String key) {
        String value = get(key);
        return value == null ? null : Integer.parseInt(value);
    }

    default long getLong(final String key, final long defaultValue) {
        String value = get(key);
        return value == null ? defaultValue : Long.parseLong(value);
    }

    default Long getLong(String key) {
        String value = get(key);
        return value == null ? null : Long.parseLong(value);
    }

    default String[] getArray(final String key, final String[] defaultValue) {
        String value = get(key);
        return value == null
                ? defaultValue
                : Arrays.stream(value.split(",")).map(String::trim).toArray(String[]::new);
    }

}
