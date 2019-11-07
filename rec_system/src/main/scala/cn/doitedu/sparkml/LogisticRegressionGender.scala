package cn.doitedu.sparkml

import cn.doitedu.commons.utils.SparkUtil
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable

/**
  * @author: 余辉
  * @blog: https://blog.csdn.net/silentwolfyh
  * @create: 2019/10/21
  * @description:
  * 1、利用逻辑回归分类算法来对用户进行消费行为性别预测
  **/
object LogisticRegressionGender {

  def main(args: Array[String]): Unit = {

    // 1、建立session连接
    Logger.getLogger("org").setLevel(Level.WARN)
    val spark: SparkSession = SparkUtil.getSparkSession(this.getClass.getSimpleName)
    import spark.implicits._
    import org.apache.spark.sql.functions._

    // 2、样本数据集
    val df: DataFrame = spark.read.option("header", true).option("inferSchema", true).csv("rec_system/data/lgreg/lgreg_sample.csv")
    df.show(10, false)

    // 3、把各个特征整成向量 UDF函数
    val tovec: UserDefinedFunction = udf((arr: mutable.WrappedArray[Double]) => {
      Vectors.dense(arr.toArray)
    })

    // 4、转为向量 lable , gid , dense(category1,--, brand1,-- day30_buy_cnts)
    val vec: DataFrame = df.select('label, 'gid, tovec(array('category1, 'category2, 'category3, 'brand1, 'brand2, 'brand3, 'day30_buy_cnts, 'day30_buy_amt)).as("features"))

    // 5、调逻辑回归算法 ，LogisticRegression
    val lgr: LogisticRegression = new LogisticRegression()
      .setLabelCol("label")
      .setFeaturesCol("features")
      .setRegParam(0.5) // 惩罚因子
      .setThreshold(0.5) // 类别判断阈值

    // 6、训练数据成模型  lgr.fit(vec)
    val model: LogisticRegressionModel = lgr.fit(vec)

    // 7、读取需要预测的数据
    val df_test: DataFrame = spark.read.option("header", true).option("inferSchema", true).csv("rec_system/data/lgreg/lgreg_test.csv")

    // 8、需要预测的数据转为特征值
    val vec_test: DataFrame = df_test.select('label, 'gid, tovec(array('category1, 'category2, 'category3, 'brand1, 'brand2, 'brand3, 'day30_buy_cnts, 'day30_buy_amt)).as("features"))

    // 9、用训练好的模型来对未知数据进行性别预测
    val res: DataFrame = model.transform(vec_test)

    res.show(10, false)

    // 10、spark关闭
    spark.close()
  }
}
