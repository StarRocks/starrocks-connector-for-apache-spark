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

import java.io.Serializable;
import java.time.ZoneId;
import java.util.Map;
import javax.annotation.Nullable;

public interface StarRocksConfig extends Serializable {

    String PREFIX = "starrocks.";
    String DORIS_PREFIX = "doris."; // to compatible with doris config

    Map<String, String> getOriginOptions();

    String[] getFeHttpUrls();
    String getFeJdbcUrl();
    String getDatabase();
    String getTable();
    String getUsername();
    String getPassword();
    int getHttpRequestRetries();
    int getHttpRequestConnectTimeoutMs();
    int getHttpRequestSocketTimeoutMs();
    ZoneId getTimeZone();

    @Nullable
    String[] getColumns();
    @Nullable
    String getColumnTypes();
}
