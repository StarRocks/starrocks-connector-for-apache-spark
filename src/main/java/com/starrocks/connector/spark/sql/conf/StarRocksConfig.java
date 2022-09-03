package com.starrocks.connector.spark.sql.conf;

import com.starrocks.connector.spark.sql.schema.StarRocksField;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public interface StarRocksConfig {

    static StarRocksConfig createConfig(final Map<String, String> options) {
        String type = options.getOrDefault(CONF_TYPE, "").toLowerCase(Locale.ROOT);
        switch (type) {
            case "write":
                return new WriteStarRocksConfig(options);
            case "read":
                return new ReadStarRocksConfig(options);
            default:
                return new SimpleStarRocksConfig(options);
        }
    }

    static ReadStarRocksConfig readConfig(final Map<String, String> options) {
        return new ReadStarRocksConfig(options);
    }

    static WriteStarRocksConfig writeConfig(final Map<String, String> options) {
        return new WriteStarRocksConfig(options);
    }

    String PREFIX = "spark.starrocks.";
    String WRITE_PREFIX = PREFIX + "write.";
    String READ_PREFIX = PREFIX + "read.";
    String INFER_PREFIX = PREFIX + "infer.";

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
    String getUsername();
    String getPassword();

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
