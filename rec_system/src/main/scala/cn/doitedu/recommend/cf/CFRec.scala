package cn.doitedu.recommend.cf

import cn.doitedu.commons.utils.SparkUtil
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.recommendation.{ALS, ALSModel}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * @author: 余辉
  * @blog: https://blog.csdn.net/silentwolfyh
  * @create: 2019/10/21
  * @description:
  **/
object CFRec {

  def main(args: Array[String]): Unit = {

    // 1、建立session连接
    Logger.getLogger("org").setLevel(Level.WARN)
    val spark: SparkSession = SparkUtil.getSparkSession(this.getClass.getSimpleName)
    import spark.implicits._
    import org.apache.spark.sql.functions._

    // 2、获取原始数据
    val ui: DataFrame = spark.read.parquet("rec_system/data/cb_out/ui")
    /**
      * +-----+---+-----+
      * |gid  |pid|score|
      * +-----+---+-----+
      * |gid02|p03|2.0  |
      * |gid03|p03|0.0  |
      * |gid03|p02|0.0  |
      * |gid03|p04|2.0  |
      */

    // 3、注册UDF函数， 字符串转为 hashCode
    val hash: UserDefinedFunction = udf((str: String) => {
      str.hashCode
    })

    // 4、数据转为 DF ： gid  pid  hash('gid)   hash('pid) score
    val ui_numeric: DataFrame = ui.select('gid, 'pid, hash('gid) as "hashgid", hash('pid) as "hashpid", 'score)

    ui_numeric.printSchema()
    ui_numeric.show(10, false)

    // 5、调算法（ALS(alternating least squares ):交替最小二乘法）
    val als: ALS = new ALS()
      .setUserCol("hashgid")
      .setItemCol("hashpid")
      .setRatingCol("score")
      .setRegParam(0.01) // 为了防止过拟合设置一个惩罚因子

    // 6、训练模型
    val model: ALSModel = als.fit(ui_numeric)

    // 7、为每一个用户，推荐n个物品
    val recForUser: DataFrame = model.recommendForAllUsers(3)
    // 8、为每一个item，推荐给n个人
    val recForItem: DataFrame = model.recommendForAllItems(3)

    // 9、结果展示
    recForUser.show(10, false)
    recForItem.show(10, false)

    // 10、spark关闭
    spark.close()
  }
}
