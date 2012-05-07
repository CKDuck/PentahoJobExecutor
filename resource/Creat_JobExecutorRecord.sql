CREATE TABLE `JobExecutorRecord` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `record_time` timestamp NULL DEFAULT NULL,
  `log_time` datetime DEFAULT NULL,
  `start_time` datetime DEFAULT NULL,
  `end_time` datetime DEFAULT NULL,
  `kitchen_command` varchar(1000) DEFAULT NULL,
  `time_spend` time DEFAULT NULL,
  `success` varchar(20) DEFAULT NULL,
  `level` varchar(20) DEFAULT NULL,
  `mode` varchar(20) DEFAULT NULL,
  `specifiedDate` varchar(100) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=MyISAM AUTO_INCREMENT=9 DEFAULT CHARSET=utf8;