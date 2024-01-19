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

package com.starrocks.connector.spark.cfg;

import com.google.common.collect.ImmutableSet;
import org.apache.commons.lang3.StringUtils;

import java.util.Map;
import java.util.Set;

import static com.starrocks.connector.spark.sql.conf.StarRocksConfigBase.KEY_FE_HTTP;

public interface ConfigurationOptions {
    // starrocks fe node address， for rest api
    String STARROCKS_FENODES = "starrocks.fenodes";

    String STARROCKS_DEFAULT_CLUSTER = "default_cluster";

    String STARROCKS_TIMEZONE = "starrocks.timezone";

    String TABLE_IDENTIFIER = "table.identifier";
    String STARROCKS_TABLE_IDENTIFIER = "starrocks.table.identifier";
    String STARROCKS_READ_FIELD = "starrocks.read.field";
    String STARROCKS_FILTER_QUERY = "starrocks.filter.query";
    String STARROCKS_FILTER_QUERY_IN_MAX_COUNT = "starrocks.filter.query.in.max.count";
    int STARROCKS_FILTER_QUERY_IN_VALUE_UPPER_LIMIT = 10000;

    String STARROCKS_USER = "starrocks.user";
    String STARROCKS_REQUEST_AUTH_USER = "starrocks.request.auth.user";
    // use password to save starrocks.request.auth.password
    // reuse credentials mask method in spark ExternalCatalogUtils#maskCredentials
    String STARROCKS_PASSWORD = "starrocks.password";
    String STARROCKS_REQUEST_AUTH_PASSWORD = "starrocks.request.auth.password";

    String STARROCKS_REQUEST_RETRIES = "starrocks.request.retries";
    String STARROCKS_REQUEST_CONNECT_TIMEOUT_MS = "starrocks.request.connect.timeout.ms";
    String STARROCKS_REQUEST_READ_TIMEOUT_MS = "starrocks.request.read.timeout.ms";
    String STARROCKS_REQUEST_QUERY_TIMEOUT_S = "starrocks.request.query.timeout.s";
    int STARROCKS_REQUEST_RETRIES_DEFAULT = 3;
    int STARROCKS_REQUEST_CONNECT_TIMEOUT_MS_DEFAULT = 30 * 1000;
    int STARROCKS_REQUEST_READ_TIMEOUT_MS_DEFAULT = 30 * 1000;
    int STARROCKS_REQUEST_QUERY_TIMEOUT_S_DEFAULT = 3600;

    String STARROCKS_TABLET_SIZE = "starrocks.request.tablet.size";
    int STARROCKS_TABLET_SIZE_DEFAULT = Integer.MAX_VALUE;
    int STARROCKS_TABLET_SIZE_MIN = 1;

    String STARROCKS_BATCH_SIZE = "starrocks.batch.size";
    int STARROCKS_BATCH_SIZE_DEFAULT = 1024;

    String STARROCKS_EXEC_MEM_LIMIT = "starrocks.exec.mem.limit";
    long STARROCKS_EXEC_MEM_LIMIT_DEFAULT = 2147483648L;

    String STARROCKS_VALUE_READER_CLASS = "starrocks.value.reader.class";

    String STARROCKS_DESERIALIZE_ARROW_ASYNC = "starrocks.deserialize.arrow.async";
    boolean STARROCKS_DESERIALIZE_ARROW_ASYNC_DEFAULT = false;

    String STARROCKS_DESERIALIZE_QUEUE_SIZE = "starrocks.deserialize.queue.size";
    int STARROCKS_DESERIALIZE_QUEUE_SIZE_DEFAULT = 64;

    static Map<String, String> makeWriteCompatibleWithRead(Map<String, String> options) {
        // user and password compatible
        processSameCompatible(ImmutableSet.of(STARROCKS_USER, STARROCKS_REQUEST_AUTH_USER, "user"), options);
        processSameCompatible(ImmutableSet.of(STARROCKS_PASSWORD, STARROCKS_REQUEST_AUTH_PASSWORD, "password"), options);
        processSameCompatible(ImmutableSet.of(STARROCKS_FENODES, KEY_FE_HTTP), options);
        return options;
    }

    static boolean processSameCompatible(Set<String> confCandidates, Map<String, String> options) {
        String value = "";
        for (String key : confCandidates) {
            if (options.containsKey(key)) {
                value = options.get(key);
            }
        }
        if (StringUtils.isNotEmpty(value)) {
            for (String key : confCandidates) {
                options.put(key, value);
            }
            return true;
        }

        return false;
    }

}
