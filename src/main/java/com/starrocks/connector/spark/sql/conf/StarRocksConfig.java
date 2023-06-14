package com.starrocks.connector.spark.sql.conf;

import java.io.Serializable;
import java.util.Map;

public interface StarRocksConfig extends Serializable {

    String PREFIX = "starrocks.";

    Map<String, String> getOriginOptions();

    String getDatabase();
    String getTable();
    String[] getColumns();
    String[] getFeHttpUrls();
    String getFeJdbcUrl();
    String getUsername();
    String getPassword();
    int getHttpRequestRetries();
    int getHttpRequestConnectTimeoutMs();
    int getHttpRequestSocketTimeoutMs();
}
