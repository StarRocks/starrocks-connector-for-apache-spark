CREATE TABLE IF NOT EXISTS `%s`.`%s`
(
    site_id   INT NOT NULL COMMENT 'id of site',
    city_code VARCHAR(20) COMMENT 'city_code of user',
    pv        BIGINT SUM DEFAULT '0' COMMENT 'total page views',
    version   BIGINT REPLACE DEFAULT '0' COMMENT 'page view version'
) ENGINE = OLAP AGGREGATE KEY(site_id, city_code)
DISTRIBUTED BY HASH(site_id)
PROPERTIES (
    'replication_num' = '1'
);