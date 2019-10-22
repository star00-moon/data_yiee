package cn.doitedu.sparkml

import cn.doitedu.commons.utils.SparkUtil
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.linalg.Vectors

import scala.collection.mutable

/**
 * @author: 余辉
 * @blog:   https://blog.csdn.net/silentwolfyh
 * @create: 2019/10/21
 * @description: 利用逻辑回归分类算法来对用户进行消费行为性别预测
 **/
object LogisticRegressionGender {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.WARN)

    val spark = SparkUtil.getSparkSession(this.getClass.getSimpleName)
    import spark.implicits._
    import org.apache.spark.sql.functions._

    val df = spark.read.option("header", true).option("inferSchema", true).csv("rec_system/data/lgreg/lgreg_sample.csv")

    df.show(10, false)

    // 把各个特征整成向量
    val tovec = udf((arr: mutable.WrappedArray[Double]) => {
      Vectors.dense(arr.toArray)
    })

    val vec = df.select('label, 'gid, tovec(array('category1, 'category2, 'category3, 'brand1, 'brand2, 'brand3, 'day30_buy_cnts, 'day30_buy_amt)).as("features"))

    // 调算法
    val lgr = new LogisticRegression()
      .setLabelCol("label")
      .setFeaturesCol("features")
      .setRegParam(0.5) // 惩罚因子
      .setThreshold(0.5) // 类别判断阈值

    val model = lgr.fit(vec)

    // 这里可以保存模型了
    val df_test = spark.read.option("header", true).option("inferSchema", true).csv("rec_system/data/lgreg/lgreg_test.csv")

    val vec_test = df_test.select('label, 'gid, tovec(array('category1, 'category2, 'category3, 'brand1, 'brand2, 'brand3, 'day30_buy_cnts, 'day30_buy_amt)).as("features"))

    // 用训练好的模型来对未知数据进行性别预测
    val res = model.transform(vec_test)

    res.show(10, false)
    spark.close()
  }
}
