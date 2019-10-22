package cn.doitedu.profile.tagextract

import cn.doitedu.commons.utils.{DictsLoader, SparkUtil}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.collection.mutable.ListBuffer


/**
  * @author: 余辉 https://blog.csdn.net/silentwolfyh
  * @create: 2019/10/18
  * @description: 消费订单
  *               1、数仓报表：用户订单统计表 标签抽取程序
  *               2、返回：gid，模块，便签，值，权重 (Long, String, String, String, Double)
  **/

case class UserOrderBean(user_id: String, first_order_time: String, last_order_time: String, first_order_ago: Int, last_order_ago: Int, month1_order_cnt: Int, month1_order_amt: Double)

object UserOrderTagsExtractor {

  def extractUserOrderTags(spark: SparkSession, path: String, idmp: collection.Map[Long, Long]): RDD[(Long, String, String, String, Double)] = {
    val bc = spark.sparkContext.broadcast(idmp)

    import spark.implicits._

    // user_id,first_order_time,last_order_time,first_order_ago,last_order_ago,month1_order_cnt,month1_order_amt
    val schema = new StructType().add("user_id", DataTypes.StringType)
      .add("first_order_time", DataTypes.StringType)
      .add("last_order_time", DataTypes.StringType)
      .add("first_order_ago", DataTypes.IntegerType)
      .add("last_order_ago", DataTypes.IntegerType)
      .add("month1_order_cnt", DataTypes.IntegerType)
      .add("month1_order_amt", DataTypes.DoubleType)


    val ds = spark.read.schema(schema).option("header", true).csv(path).as[UserOrderBean]
    val res: RDD[(Long, String, String, String, Double)] = ds.rdd
      .mapPartitions(iter => {
        val idmpDict = bc.value
        iter.map(bean => {

          val lst = new ListBuffer[(Long, String, String, String, Double)]

          val user_id = bean.user_id
          //val gid = idmpDict.get(user_id.hashCode.toLong).get
          val gid = 1096698831L

          lst += ((gid, "M004", "T404", bean.first_order_time, -9999.9))
          lst += ((gid, "M004", "T405", bean.last_order_time, -9999.9))
          lst += ((gid, "M004", "T406", bean.first_order_ago.toString, -9999))
          lst += ((gid, "M004", "T407", bean.last_order_ago.toString, -9999))
          lst += ((gid, "M004", "T408", bean.month1_order_cnt.toString, -9999))
          lst += ((gid, "M004", "T409", bean.month1_order_amt.toString, -9999))

          lst.toIterator
        })
      })
      .flatMap(iter => iter)

    res

  }


  /**
    * 测试代码
    *
    * @param args
    */
  def main(args: Array[String]): Unit = {
    val spark = SparkUtil.getSparkSession("")
    import spark.implicits._

    val idmp = DictsLoader.loadIdmpDict(spark, "user_profile/data/output/idmp/day01")
    extractUserOrderTags(spark, "user_profile/data/t_ads_user_order_tag", idmp)
      .toDF("gid", "module", "tagname", "tagvalue", "score")
      .show(10, false)
  }


}
