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

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZoneId;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;

import static com.starrocks.connector.spark.cfg.ConfigurationOptions.STARROCKS_PASSWORD;
import static com.starrocks.connector.spark.cfg.ConfigurationOptions.STARROCKS_REQUEST_CONNECT_TIMEOUT_MS;
import static com.starrocks.connector.spark.cfg.ConfigurationOptions.STARROCKS_REQUEST_READ_TIMEOUT_MS;
import static com.starrocks.connector.spark.cfg.ConfigurationOptions.STARROCKS_REQUEST_RETRIES;
import static com.starrocks.connector.spark.cfg.ConfigurationOptions.STARROCKS_TABLE_IDENTIFIER;
import static com.starrocks.connector.spark.cfg.ConfigurationOptions.STARROCKS_TIMEZONE;
import static com.starrocks.connector.spark.cfg.ConfigurationOptions.STARROCKS_USER;
import static com.starrocks.connector.spark.rest.RestService.parseIdentifier;

public abstract class StarRocksConfigBase implements StarRocksConfig {

    private static final Logger LOG = LoggerFactory.getLogger(StarRocksConfigBase.class);

    // reuse some configurations in ConfigurationOptions
    public static final String KEY_FE_HTTP = PREFIX + "fe.http.url";
    static final String KEY_FE_JDBC = PREFIX + "fe.jdbc.url";
    static final String KEY_TABLE_IDENTIFIER = STARROCKS_TABLE_IDENTIFIER;
    static final String KEY_USERNAME = STARROCKS_USER;
    static final String KEY_PASSWORD = STARROCKS_PASSWORD;
    static final String KEY_REQUEST_RETRIES = STARROCKS_REQUEST_RETRIES;
    static final String KEY_REQUEST_CONNECT_TIMEOUT = STARROCKS_REQUEST_CONNECT_TIMEOUT_MS;
    static final String KEY_REQUEST_SOCKET_TIMEOUT = STARROCKS_REQUEST_READ_TIMEOUT_MS;
    public static final String KEY_COLUMNS = PREFIX + "columns";
    // Specify the spark type of the column in starrocks if you want to customize the data type
    // mapping between spark and starrocks. For example, there are two columns `c0`(DATE) and
    // `c1`(DATETIME) in starrocks, you can set KEY_COLUMN_TYPES to "c0 STRING, c1 STRING" to
    // map them to spark's STRING type, rather than the default DATE and TIMESTAMP. We use spark's
    // StructType#fromDDL to parse the schema, so the format should follow the format of the output
    // of StructType#toDDL. You only need to specify the columns that not follow the default
    // data type mapping instead of all columns
    public static final String KEY_COLUMN_TYPES = PREFIX + "column.types";

    protected final Map<String, String> originOptions;

    private String[] feHttpUrls;
    private String feJdbcUrl;
    private String username;
    private String password;
    private String database;
    private String table;
    @Nullable
    private String[] columns;
    @Nullable
    private String columnTypes;
    private int httpRequestRetries;
    private int httpRequestConnectTimeoutMs;
    private int httpRequestSocketTimeoutMs;
    private ZoneId timeZone;

    public StarRocksConfigBase(Map<String, String> options) {
        this.originOptions = new HashMap<>(options);
        load();
    }

    private void load() {
        this.feHttpUrls = getArray(KEY_FE_HTTP, new String[0]);
        this.feJdbcUrl = get(KEY_FE_JDBC);
        this.username = get(KEY_USERNAME);
        this.password = get(KEY_PASSWORD);
        String identifier = get(KEY_TABLE_IDENTIFIER);
        if (StringUtils.isNotEmpty(identifier)) {
            String[] parsedResult = parseIdentifier(identifier, LOG);
            this.database = parsedResult[0];
            this.table = parsedResult[1];
        }
        this.columns = getArray(KEY_COLUMNS, null);
        this.columnTypes = get(KEY_COLUMN_TYPES);
        this.httpRequestRetries = getInt(KEY_REQUEST_RETRIES, 3);
        this.httpRequestConnectTimeoutMs = getInt(KEY_REQUEST_CONNECT_TIMEOUT, 30000);
        this.httpRequestSocketTimeoutMs = getInt(KEY_REQUEST_SOCKET_TIMEOUT, 30000);

        String tz = get(STARROCKS_TIMEZONE);
        this.timeZone = tz == null ? ZoneId.systemDefault() : ZoneId.of(get(STARROCKS_TIMEZONE));
    }

    @Override
    public Map<String, String> getOriginOptions() {
        return originOptions;
    }

    @Override
    public String[] getFeHttpUrls() {
        return feHttpUrls;
    }

    @Override
    public String getFeJdbcUrl() {
        return feJdbcUrl;
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
    public int getHttpRequestRetries() {
        return httpRequestRetries;
    }

    @Override
    public int getHttpRequestConnectTimeoutMs() {
        return httpRequestConnectTimeoutMs;
    }

    @Override
    public int getHttpRequestSocketTimeoutMs() {
        return httpRequestSocketTimeoutMs;
    }

    @Override
    public ZoneId getTimeZone() {
        return timeZone;
    }

    @Override
    @Nullable
    public String[] getColumns() {
        return columns;
    }

    @Override
    @Nullable
    public String getColumnTypes() {
        return columnTypes;
    }

    protected String get(final String key) {
        return get(key, null);
    }

    protected String get(final String key, String defaultValue) {
        String value = getOriginOptions().get(key);
        return value != null ? value : defaultValue;
    }

    protected boolean getBoolean(final String key, final boolean defaultValue) {
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

    protected int getInt(final String key, final int defaultValue) {
        String value = get(key);
        return value == null ? defaultValue : Integer.parseInt(get(key));
    }

    protected Integer getInt(String key) {
        String value = get(key);
        return value == null ? null : Integer.parseInt(value);
    }

    protected long getLong(final String key, final long defaultValue) {
        String value = get(key);
        return value == null ? defaultValue : Long.parseLong(value);
    }

    protected Long getLong(String key) {
        String value = get(key);
        return value == null ? null : Long.parseLong(value);
    }

    protected String[] getArray(final String key, final String[] defaultValue) {
        String value = get(key);
        return value == null
                ? defaultValue
                : Arrays.stream(value.split(",")).map(String::trim).toArray(String[]::new);
    }

}
