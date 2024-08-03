CREATE TABLE IF NOT EXISTS `%s`.`%s`
(
    id    INT,
    name STRING,
    score INT
) ENGINE = OLAP PRIMARY KEY(`id`)
DISTRIBUTED BY HASH(`id`) BUCKETS 2
PROPERTIES (
    "replication_num" = "1"
);