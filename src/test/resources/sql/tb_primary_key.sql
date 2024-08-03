CREATE TABLE IF NOT EXISTS `%s`.`%s`
(
    user_id BIGINT       NOT NULL COMMENT 'user id',
    name    VARCHAR(32)  NOT NULL COMMENT 'user name',
    age     TINYINT      NOT NULL COMMENT 'user age',
    sex     TINYINT      NOT NULL COMMENT 'user sex',
    email   VARCHAR(256) NULL COMMENT 'email'
) ENGINE = OLAP PRIMARY KEY (user_id)
DISTRIBUTED BY HASH(user_id)
ORDER BY(`name`)
PROPERTIES (
    "replication_num" = "1"
);