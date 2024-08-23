CREATE TABLE IF NOT EXISTS `%s`.`%s`
(
    order_id    BIGINT NOT NULL COMMENT 'id of an order',
    order_state INT COMMENT 'state of an order',
    total_price BIGINT COMMENT 'price of an order'
) ENGINE = OLAP UNIQUE KEY(order_id)
DISTRIBUTED BY HASH(order_id) BUCKETS 2
PROPERTIES (
    "replication_num" = "1"
);