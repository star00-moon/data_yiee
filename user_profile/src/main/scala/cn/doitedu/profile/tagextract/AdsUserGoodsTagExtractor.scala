package cn.doitedu.profile.tagextract

import cn.doitedu.commons.utils.{DictsLoader, SparkUtil}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

import scala.collection.mutable.ListBuffer


/**
  * @author: 余辉
  * @blog: https://blog.csdn.net/silentwolfyh
  * @create: 2019/10/19
  * @description: 消费商品退拒表
  *               1、用户订单商品退拒分析报表 标签抽取
  *               2、返回：gid，模块，便签，值，权重 (Long, String, String, String, Double)
  **/
case class AdsUserGoods(
                         user_id: Long,
                         p_sales_cnt: Long,
                         p_sales_amt: Double,
                         p_sales_cut_amt: Double,
                         h_sales_cnt: Long,
                         h_sales_amt: Double,
                         h_sales_cut_amt: Double,
                         return_cnt: Long,
                         return_amt: Double,
                         reject_cnt: Long,
                         reject_amt: Double,
                         common_first_cat: Long,
                         common_second_cat: Long,
                         common_third_cat: Long
                       )


object AdsUserGoodsTagExtractor {


  def extractUserGoodsTags(spark: SparkSession, path: String, idmp: collection.Map[Long, Long]): RDD[(Long, String, String, String, Double)] = {

    val schema = new StructType(Array(
      new StructField("user_id", DataTypes.LongType),
      new StructField("p_sales_cnt", DataTypes.LongType),
      new StructField("p_sales_amt", DataTypes.DoubleType),
      new StructField("p_sales_cut_amt", DataTypes.DoubleType),
      new StructField("h_sales_cnt", DataTypes.LongType),
      new StructField("h_sales_amt", DataTypes.DoubleType),
      new StructField("h_sales_cut_amt", DataTypes.DoubleType),
      new StructField("return_cnt", DataTypes.LongType),
      new StructField("return_amt", DataTypes.DoubleType),
      new StructField("reject_cnt", DataTypes.LongType),
      new StructField("reject_amt", DataTypes.DoubleType),
      new StructField("common_first_cat", DataTypes.LongType),
      new StructField("common_second_cat", DataTypes.LongType),
      new StructField("common_third_cat", DataTypes.LongType)
    ))


    import spark.implicits._
    val df = spark.read.schema(schema).option("header", true).csv(path)
    val bc = spark.sparkContext.broadcast(idmp)

    val ds = df.as[AdsUserGoods]

    // 你给我一个bean，我还你一堆标签
    ds
      .rdd
      .mapPartitions(iter => {
        val idmpDict = bc.value
        iter.map(bean => {

          val lst = new ListBuffer[(Long, String, String, String, Double)]

          val user_id = bean.user_id
          val gid = idmpDict.get(user_id.hashCode.toLong).get

          lst += ((gid, "M020", "T0201", bean.p_sales_cnt.toString, -9999.9))
          lst += ((gid, "M020", "T0202", bean.p_sales_amt.toString, -9999.9))
          lst += ((gid, "M020", "T0203", bean.p_sales_cut_amt.toString, -9999.9))
          lst += ((gid, "M020", "T0204", bean.h_sales_cnt.toString, -9999.9))
          lst += ((gid, "M020", "T0205", bean.h_sales_amt.toString, -9999.9))
          lst += ((gid, "M020", "T0206", bean.h_sales_cut_amt.toString, -9999.9))
          lst += ((gid, "M020", "T0207", bean.return_cnt.toString, -9999.9))
          lst += ((gid, "M020", "T0208", bean.return_amt.toString, -9999.9))
          lst += ((gid, "M020", "T0209", bean.reject_cnt.toString, -9999.9))
          lst += ((gid, "M020", "T0210", bean.reject_amt.toString, -9999.9))
          lst += ((gid, "M020", "T0211", bean.common_first_cat.toString, -9999.9))
          lst += ((gid, "M020", "T0212", bean.common_second_cat.toString, -9999.9))
          lst += ((gid, "M020", "T0213", bean.common_third_cat.toString, -9999.9))

          lst.toIterator
        })
      })
      .flatMap(iter => iter)
  }

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkUtil.getSparkSession(this.getClass.getSimpleName)
    import spark.implicits._
    val ads_user_goods_path = "user_profile/data/t_user_goods"
    val idmp_path = "user_profile/data/output/idmp/day01"
    val idmp = DictsLoader.loadIdmpDict(spark, idmp_path)
    val goodsLogs: RDD[(Long, String, String, String, Double)] = extractUserGoodsTags(spark, ads_user_goods_path, idmp)
    goodsLogs.take(10).foreach(println)
  }
}
