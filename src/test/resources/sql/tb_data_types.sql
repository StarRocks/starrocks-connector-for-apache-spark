CREATE TABLE IF NOT EXISTS `%s`.`%s`
(
    boolean_value1 BOOLEAN         NOT NULL,
    boolean_value2 BOOLEAN         NOT NULL,
    tinyint_value  TINYINT         NOT NULL,
    smallint_value SMALLINT        NOT NULL,
    int_value      INT             NOT NULL,
    bigint_value   BIGINT          NOT NULL,
    largeint_lower LARGEINT NOT NULL,
    largeint_upper LARGEINT NOT NULL,
    float_value    FLOAT           NOT NULL,
    double_value   DOUBLE          NOT NULL,
    decimal_value1 DECIMAL(20, 10) NOT NULL,
    decimal_value2 DECIMAL(32, 10) NOT NULL,
    decimal_value3 DECIMAL(10, 6)  NOT NULL,
    date_value     DATE            NOT NULL,
    datetime_value DATETIME        NOT NULL
) ENGINE = OLAP DUPLICATE KEY (boolean_value1, boolean_value2, tinyint_value, smallint_value, int_value, bigint_value)
DISTRIBUTED BY HASH(boolean_value1, boolean_value2, tinyint_value, smallint_value, int_value, bigint_value) BUCKETS 4
PROPERTIES(
    "replication_num" = "1"
);