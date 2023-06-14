package com.starrocks.connector.spark.sql.conf;

import java.util.Map;

public class SimpleStarRocksConfig extends StarRocksConfigBase {

    private static final long serialVersionUID = 1L;

    public SimpleStarRocksConfig(Map<String, String> options) {
        super(options);
    }
}
