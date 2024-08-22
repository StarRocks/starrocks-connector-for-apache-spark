CREATE TABLE IF NOT EXISTS `%s`.`%s`
(
    id      bigint,
    user_id bigint,
    city    varchar(20) NOT NULL
) DUPLICATE KEY(id)
PARTITION BY (city)
DISTRIBUTED BY HASH(`id`)
PROPERTIES (
    "replication_num" = "1"
);