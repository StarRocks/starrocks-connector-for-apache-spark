// Modifications Copyright 2021 StarRocks Limited.
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;

import static com.starrocks.connector.spark.cfg.ConfigurationOptions.STARROCKS_FENODES;
import static com.starrocks.connector.spark.cfg.ConfigurationOptions.STARROCKS_PASSWORD;
import static com.starrocks.connector.spark.cfg.ConfigurationOptions.STARROCKS_REQUEST_CONNECT_TIMEOUT_MS;
import static com.starrocks.connector.spark.cfg.ConfigurationOptions.STARROCKS_REQUEST_READ_TIMEOUT_MS;
import static com.starrocks.connector.spark.cfg.ConfigurationOptions.STARROCKS_REQUEST_RETRIES;
import static com.starrocks.connector.spark.cfg.ConfigurationOptions.STARROCKS_TABLE_IDENTIFIER;
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

    protected final Map<String, String> originOptions;

    private String[] feHttpUrls;
    private String feJdbcUrl;
    private String username;
    private String password;
    private String database;
    private String table;
    @Nullable
    private String[] columns;
    private int httpRequestRetries;
    private int httpRequestConnectTimeoutMs;
    private int httpRequestSocketTimeoutMs;

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
        try {
            String[] parsedResult = parseIdentifier(identifier, LOG);
            this.database = parsedResult[0];
            this.table = parsedResult[1];
        } catch (Exception e) {
            LOG.error("Failed to parse table identifier: {}", identifier, e);
            throw new RuntimeException(e);
        }
        this.columns = getArray(KEY_COLUMNS, null);
        this.httpRequestRetries = getInt(KEY_REQUEST_RETRIES, 3);
        this.httpRequestConnectTimeoutMs = getInt(KEY_REQUEST_CONNECT_TIMEOUT, 30000);
        this.httpRequestSocketTimeoutMs = getInt(KEY_REQUEST_SOCKET_TIMEOUT, 30000);
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
    @Nullable
    public String[] getColumns() {
        return columns;
    }

    protected String get(final String key) {
        return getOriginOptions().get(key);
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
