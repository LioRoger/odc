--
-- Add supervisor_endpoint table to maintain supervisor agent info, this table will bind to resource_resource
--
CREATE TABLE `supervisor_endpoint` (
    `id` bigint(20) NOT NULL AUTO_INCREMENT,
    `host` varchar(256)  NOT NULL COMMENT 'host of supervisor',
    `port` int(11)  NOT NULL COMMENT 'port of supervisor',
    `status` varchar(64)  NOT NULL COMMENT 'status of supervisor, contains PREPAREING,AVAILABLE,DESTROYED,UNAVAILABLE，ABANDON',
    `loads` int(11)  NOT NULL COMMENT 'load of supervisor',
    `resource_id` bigint(20) NOT NULL COMMENT 'resource id related to resource_resource table, -1 means not related',
    `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'create time',
    `update_time` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'update time',
    PRIMARY KEY (`id`),
    UNIQUE KEY host_and_port (`host`, `port`),
    KEY `status_index` (`status`)
);