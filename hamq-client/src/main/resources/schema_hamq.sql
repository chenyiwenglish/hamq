CREATE TABLE `message_queue_info` (
  `queue_id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '队列id',
  `max_retry_count` int(11) NOT NULL DEFAULT '0' COMMENT '最大重试次数',
  `fading_type` int(11) NOT NULL DEFAULT '0' COMMENT '衰减类型(0不衰减1线性2平方3立方4指数)',
  `status` int(11) NOT NULL DEFAULT '0' COMMENT '队列状态(0运行1暂停)',
  `owner` varchar(64) NOT NULL DEFAULT '' COMMENT '所有者',
  `description` varchar(512) NOT NULL DEFAULT '' COMMENT '描述',
  `update_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`queue_id`)
) ENGINE=InnoDB AUTO_INCREMENT=2 DEFAULT CHARSET=utf8 COMMENT='队列表';

CREATE TABLE `message_queue` (
  `message_id` bigint(20) NOT NULL DEFAULT '0' COMMENT '消息id',
  `queue_id` bigint(20) NOT NULL DEFAULT '0' COMMENT '队列id',
  `message_body` text NOT NULL COMMENT '消息体',
  `extra_info` varchar(4096) NOT NULL DEFAULT '' COMMENT '附加信息(目前是日志reqId相关)',
  `idc` varchar(16) NOT NULL DEFAULT '' COMMENT '机房idc',
  `retry_count` int(11) NOT NULL DEFAULT '0' COMMENT '当前重试次数',
  `lock_time` datetime NOT NULL DEFAULT '0000-00-00 00:00:00' COMMENT '锁定时间',
  `consume_time` bigint(20) NOT NULL DEFAULT '0' COMMENT '消费时间',
  PRIMARY KEY (`message_id`),
  KEY `idx_queue_id` (`queue_id`),
  KEY `idx_consume_time` (`consume_time`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='消息表';

