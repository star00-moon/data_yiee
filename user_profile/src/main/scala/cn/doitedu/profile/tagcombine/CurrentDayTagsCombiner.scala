package cn.doitedu.profile.tagcombine

import java.io.File

import cn.doitedu.commons.utils.{DictsLoader, FileUtils, SparkUtil}
import cn.doitedu.profile.tagextract.{AdsUserGoodsTagExtractor, CmccTagExtractor, DemoTagsReader, DspTagExtractor, EventLogTagExtractor, UserOrderTagsExtractor}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession


/**
  * @author: 余辉
  * @blog: https://blog.csdn.net/silentwolfyh
  * @create: 2019/10/19
  * @description:
  * 1、当日各数据源所抽取的标签的聚合程序
  * 2、要注意的是：  标签的数据模型，有若干种：
  * 3、gid/模块名/标签名/标签值/权重值
  * 4、gid/模块名/标签名/标签值/-9999.9
  * 5、某其它变种
  **/
object CurrentDayTagsCombiner extends App {

  private val spark: SparkSession = SparkUtil.getSparkSession(this.getClass.getSimpleName)

  import spark.implicits._

  // 调用各类抽取器抽取标签, 真实数据使用
    val eventlog_path = "user_profile/data/output/eventlog/day01"
    val cmcclog_path = "user_profile/data/output/cmcc/day01"
    val dsplog_path = "user_profile/data/output/dsplog/day01"
    val ads_user_goods_path = "user_profile/data/t_user_goods"
    val ads_user_order_path = "user_profile/data/t_ads_user_order_tag"
    val idmp_path = "user_profile/data/output/idmp/day01"

    val eventTags = EventLogTagExtractor.extractEventLogTags(spark, eventlog_path)
    val cmccTags = CmccTagExtractor.extractCmccTags(spark, cmcclog_path)
    val dspTags = DspTagExtractor.extractDspTags(spark, dsplog_path)
    val idmp = DictsLoader.loadIdmpDict(spark, idmp_path)
    val userGoodsTags = AdsUserGoodsTagExtractor.extractUserGoodsTags(spark, ads_user_goods_path, idmp)
    val userOrderTags = UserOrderTagsExtractor.extractUserOrderTags(spark, ads_user_order_path, idmp)

  // 1、调用各类抽取器抽取标签, 测试数据使用
//  val cmccTags = DemoTagsReader.readCmccTags(spark)
//  val eventTags = DemoTagsReader.readEventTags(spark)
//  val dspTags = DemoTagsReader.readDspTags(spark)
//  val userGoodsTags = DemoTagsReader.readUserGoodsTags(spark)
//  val userOrderTags = DemoTagsReader.readUserOrderTags(spark)

  // 2、通过union整合各类数据源的标签  ，转为DataFrame 【"gid", "tag_module", "tag_name", "tag_value", "weight"】
  val allTags = eventTags
    .union(cmccTags)
    .union(dspTags)
    .union(userGoodsTags)
    .union(userOrderTags)
    .toDF("gid", "tag_module", "tag_name", "tag_value", "weight")

  // 3、分离各类需要不同处理方式的标签， "weight != -9999.9" 和 "weight = -9999.9"
  val haveWeight = allTags.where("weight != -9999.9")
  val noWeight = allTags.where("weight = -9999.9")

  // 4、对有权重的标签聚合
  val haveWeightResult = haveWeight
    .groupBy("gid", "tag_module", "tag_name", "tag_value")
    .agg("weight" -> "count").withColumnRenamed("count(weight)", "weight")

  // 5、整合最终结果（有权重 + 无权重）
  val currentDayTags = haveWeightResult.union(noWeight)
  currentDayTags.show(60, false)

  // 6、保存标签
  FileUtils.deleteDir(new File("user_profile/data/output/tags/day02"))
  currentDayTags.coalesce(1).write.parquet("user_profile/data/output/tags/day02")
  spark.close()
}
