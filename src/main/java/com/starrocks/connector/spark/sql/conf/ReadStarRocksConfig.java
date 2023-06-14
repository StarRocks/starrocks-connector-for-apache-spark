package com.starrocks.connector.spark.sql.conf;

import java.util.Map;

public class ReadStarRocksConfig extends StarRocksConfigBase {

    private static final long serialVersionUID = 1L;

    public ReadStarRocksConfig(Map<String, String> options) {
        super(options);
    }
}
