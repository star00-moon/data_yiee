package cn.doitedu.recomment.cb

import cn.doitedu.commons.utils.SparkUtil
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types.{DataTypes, StructType}

/**
  * @author: 余辉
  * @blog: https://blog.csdn.net/silentwolfyh
  * @create: 2019/10/21
  * @description: 用户对物品的喜好度计算
  **/
object RateScore {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)

    //  把数据变成:   gid   pid   score
    val spark = SparkUtil.getSparkSession(this.getClass.getSimpleName)

    // 加载行为日志数据
    val schema = new StructType()
      .add("gid", DataTypes.StringType)
      .add("event_type", DataTypes.StringType)
      .add("event", DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType))

    val event = spark.read.schema(schema).json("rec_system/data/ui_rate/event.log")
    event.printSchema()
    event.show(10, false)

    event.createTempView("event")

    /**
      * pv事件给1分
      * addcart事件给2分
      * 收藏事件给1分
      * 分享事件给1分
      * 推荐事件给3分
      * .......
      */
    val event_socre = spark.sql(
      """
        |
        |select
        |gid,
        |event['pid'] as pid,
        |case event_type
        | when 'pv' then 1
        | when 'add_cart' then 2
        | when 'rate' then event['score']-3
        | else 0
        |end as score
        |
        |from event
        |
      """.stripMargin)

    event_socre.show(10, false)


    /**
      * 处理评论数据bayes分类结果
      */
    val comment = spark.read.option("header", true).csv("rec_system/data/ui_rate/u.comment.dat")
    comment.createTempView("cmt")
    val cmt_score = spark.sql(
      """
        |
        |select
        |gid,
        |pid,
        |case label
        | when '0' then -1
        | when '1' then 0
        | when '2' then 1
        | else 0
        |end as score
        |from cmt
        |
      """.stripMargin)

    cmt_score.show(10, false)

    import spark.implicits._
    import org.apache.spark.sql.functions._

    val score_sum = event_socre.union(cmt_score)
      .groupBy('gid, 'pid)
      .agg("score" -> "sum").withColumnRenamed("sum(score)", "score")

    score_sum.show(10, false)

    // 保存UI评分矩阵 ： user-item的平均指数
    score_sum.coalesce(1).write.parquet("rec_system/data/cb_out/ui")
    spark.close()
  }
}