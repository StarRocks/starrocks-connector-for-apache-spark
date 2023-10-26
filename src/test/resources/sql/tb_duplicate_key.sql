CREATE TABLE IF NOT EXISTS `%s`.`%s`
(
    event_time BIGINT NOT NULL COMMENT 'timestamp of event',
    event_type INT    NOT NULL COMMENT 'type of event',
    user_id    INT COMMENT 'id of user',
    device     VARCHAR(128) COMMENT 'device'
) ENGINE = OLAP DUPLICATE KEY(event_time, event_type)
DISTRIBUTED BY HASH(user_id)
PROPERTIES (
    "replication_num" = "1"
);