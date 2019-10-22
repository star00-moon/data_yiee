package cn.doitedu.recommend.cf

import cn.doitedu.commons.utils.SparkUtil
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.recommendation.ALS

/**
  * @author: 余辉
  * @blog: https://blog.csdn.net/silentwolfyh
  * @create: 2019/10/21
  * @description:
  **/
object CFRec {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.WARN)

    val spark = SparkUtil.getSparkSession(this.getClass.getSimpleName)
    import spark.implicits._
    import org.apache.spark.sql.functions._

    /**
      * +-----+---+-----+
      * |gid  |pid|score|
      * +-----+---+-----+
      * |gid02|p03|2.0  |
      * |gid03|p03|0.0  |
      * |gid03|p02|0.0  |
      * |gid03|p04|2.0  |
      */
    val ui = spark.read.parquet("rec_system/data/cb_out/ui")

    val hash = udf((str: String) => {
      str.hashCode
    })
    val ui_numeric = ui.select('gid, 'pid, hash('gid) as "hashgid", hash('pid) as "hashpid", 'score)

    ui_numeric.printSchema()
    ui_numeric.show(10, false)

    // 调算法（ALS(alternating least squares ):交替最小二乘法）
    val als = new ALS()
      .setUserCol("hashgid")
      .setItemCol("hashpid")
      .setRatingCol("score")
      .setRegParam(0.01) // 为了防止过拟合设置一个惩罚因子

    val model = als.fit(ui_numeric)

    // 为每一个用户，推荐n个物品
    val recForUser = model.recommendForAllUsers(3)
    // 为每一个item，推荐给n个人
    val recForItem = model.recommendForAllItems(3)

    recForUser.show(10, false)
    recForItem.show(10, false)

    spark.close()
  }
}
