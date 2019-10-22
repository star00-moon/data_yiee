/*
Navicat MySQL Data Transfer

Source Server         : localhost
Source Server Version : 50715
Source Host           : localhost:3306
Source Database       : yieemeta

Target Server Type    : MYSQL
Target Server Version : 50715
File Encoding         : 65001

Date: 2019-09-06 12:07:57
*/

SET FOREIGN_KEY_CHECKS=0;

-- ----------------------------
-- Table structure for transaction_route
-- ----------------------------
DROP TABLE IF EXISTS `transaction_route`;
CREATE TABLE `transaction_route` (
  `tid` varchar(255) NOT NULL,
  `step` int(11) NOT NULL,
  `step_name` varchar(255) DEFAULT NULL,
  `event_id` varchar(255) NOT NULL,
  `pre_event_id` varchar(255) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- ----------------------------
-- Records of transaction_route
-- ----------------------------
INSERT INTO `transaction_route` VALUES ('T101', '1', '步骤1', 'A', 'null');
INSERT INTO `transaction_route` VALUES ('T101', '2', '步骤2', 'B', 'A');
INSERT INTO `transaction_route` VALUES ('T101', '3', '步骤3', 'C', 'B');
INSERT INTO `transaction_route` VALUES ('T102', '1', '步骤1', 'D', 'null');
INSERT INTO `transaction_route` VALUES ('T102', '2', '步骤2', 'B', 'D');
INSERT INTO `transaction_route` VALUES ('T102', '3', '步骤3', 'C', 'B');
