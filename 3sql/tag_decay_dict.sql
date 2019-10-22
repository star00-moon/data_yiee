DROP DATABASE yieemeta;

CREATE DATABASE IF NOT EXISTS yieemeta default charset utf8 COLLATE utf8_general_ci;

USE yieemeta;

DROP TABLE IF EXISTS `tag_decay_dict`;
CREATE TABLE `tag_decay_dict`
(
    `tag_module` varchar(255) DEFAULT NULL,
    `tag_name`   varchar(255) DEFAULT NULL,
    `decay`      double       DEFAULT '1'
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8;

INSERT INTO `tag_decay_dict`
VALUES ('M000', 'T001', '0.9');
INSERT INTO `tag_decay_dict`
VALUES ('M000', 'T002', '1');
INSERT INTO `tag_decay_dict`
VALUES ('M001', 'T101', '0.9');
INSERT INTO `tag_decay_dict`
VALUES ('M012', 'T121', '0.85');