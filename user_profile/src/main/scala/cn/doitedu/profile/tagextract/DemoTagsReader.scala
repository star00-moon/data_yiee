package cn.doitedu.profile.tagextract

import cn.doitedu.commons.utils.SparkUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}


/**
  * @author: 余辉
  * @blog: https://blog.csdn.net/silentwolfyh
  * @create: 2019/10/22
  *          1、提取各个标签，返回：gid，模块【tag_module】，便签【tag_name】，值【tag_value】，权重【weight】
  *          2、gid的返回类型为 Long，权重的返回类型为 Double
  *          3、读取Event标签 ： user_profile/demodata/tags/day02/eventtags
  *          4、读取竞价日志标签  ：user_profile/demodata/tags/day02/dsptags
  *          5、读取移动数据：user_profile/demodata/tags/day02/cmcctags
  *          6、消费商品退拒表:user_profile/demodata/tags/day02/usergoodstags
  *          7、消费订单:user_profile/demodata/tags/day02/userordertags
  **/


object DemoTagsReader {

  //读取Event标签
  def readEventTags(spark: SparkSession) = {
    import spark.implicits._
    val event = spark.read.option("header", true).csv("user_profile/demodata/tags/day02/eventtags")

    event.rdd.map({
      case Row(gid: String, tag_module: String, tag_name: String, tag_value: String, weight: String) =>
        (gid.toLong, tag_module, tag_name, tag_value, weight.toDouble)
    })
  }

  //读取竞价日志标签
  def readDspTags(spark: SparkSession) = {
    import spark.implicits._
    val event = spark.read.option("header", true).csv("user_profile/demodata/tags/day02/dsptags")

    event.rdd.map({
      case Row(gid: String, tag_module: String, tag_name: String, tag_value: String, weight: String) =>
        (gid.toLong, tag_module, tag_name, tag_value, weight.toDouble)
    })
  }

  //读取移动数据
  def readCmccTags(spark: SparkSession) = {
    import spark.implicits._
    val event = spark.read.option("header", true).csv("user_profile/demodata/tags/day02/cmcctags")

    event.rdd.map({
      case Row(gid: String, tag_module: String, tag_name: String, tag_value: String, weight: String) =>
        (gid.toLong, tag_module, tag_name, tag_value, weight.toDouble)
    })
  }

  //消费商品退拒表
  def readUserGoodsTags(spark: SparkSession) = {
    import spark.implicits._
    val event = spark.read.option("header", true).csv("user_profile/demodata/tags/day02/usergoodstags")

    event.rdd.map({
      case Row(gid: String, tag_module: String, tag_name: String, tag_value: String, weight: String) =>
        (gid.toLong, tag_module, tag_name, tag_value, weight.toDouble)
    })
  }

  //消费订单
  def readUserOrderTags(spark: SparkSession) = {
    import spark.implicits._
    val event = spark.read.option("header", true).csv("user_profile/demodata/tags/day02/userordertags")

    event.rdd.map({
      case Row(gid: String, tag_module: String, tag_name: String, tag_value: String, weight: String) =>
        (gid.toLong, tag_module, tag_name, tag_value, weight.toDouble)
    })
  }

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkUtil.getSparkSession(this.getClass.getSimpleName)
    val order: RDD[(Long, String, String, String, Double)] = readUserOrderTags(spark)
    order.foreach(println)
  }
}
