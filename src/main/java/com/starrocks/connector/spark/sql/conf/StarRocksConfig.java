package com.starrocks.connector.spark.sql.conf;

import java.io.Serializable;
import java.util.Map;
import javax.annotation.Nullable;

public interface StarRocksConfig extends Serializable {

    String PREFIX = "starrocks.";

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
    @Nullable
    String[] getColumns();
}
