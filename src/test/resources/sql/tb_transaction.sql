CREATE TABLE IF NOT EXISTS `%s`.`%s`
(
    user_id     BIGINT COMMENT 'user id',
    action_time BIGINT NOT NULL COMMENT 'action timestamp',
    action_type INT    NOT NULL COMMENT 'action type',
    source_ip   CHAR(16) COMMENT 'user ip',
    action_tag LARGEINT NOT NULL COMMENT 'action tag',
    detail STRING COMMENT 'action detail'
) ENGINE = OLAP DUPLICATE KEY(user_id, action_time, action_type)
DISTRIBUTED BY HASH(user_id)
PROPERTIES (
    "replication_num" = "1"
);