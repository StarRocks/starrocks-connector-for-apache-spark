CREATE TABLE IF NOT EXISTS `%s`.`%s`
(
    event_time BIGINT NOT NULL COMMENT 'timestamp of event',
    event_type INT    NOT NULL COMMENT 'type of event',
    user_id    INT COMMENT 'id of user',
    city       VARCHAR(32) COMMENT 'city'
) ENGINE = OLAP DUPLICATE KEY(event_time, event_type)
PARTITION BY RANGE (user_id) (
    PARTITION p1 VALUES LESS THAN ("1000"),
    PARTITION p2 VALUES LESS THAN ("2000"),
    PARTITION p3 VALUES LESS THAN ("3000")
)
DISTRIBUTED BY HASH(user_id) BUCKETS 1
PROPERTIES (
    "replication_num" = "1"
);