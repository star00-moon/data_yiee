package cn.doitedu.sparkml.demos

import cn.doitedu.commons.utils.SparkUtil
import org.apache.spark.ml.feature.{MaxAbsScaler, MinMaxScaler, Normalizer, StandardScaler}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
  * @author: 余辉
  * @blog: https://blog.csdn.net/silentwolfyh
  * @create: 2019/10/21
  * @description:
  * 1、向量规范化api示例
  * 2、参考: https://yuhui.blog.csdn.net/article/details/102664018
  **/
object VectorNormDemo {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkUtil.getSparkSession(this.getClass.getSimpleName)
    import spark.implicits._

    // 将原始数据向量化  header  inferSchema  .toDF("id", "features")
    val df: DataFrame = spark.read
      .option("header", true)
      .option("inferSchema", true)
      .csv("rec_system/demodata/vecdata/vec.csv")
      .map({
        case Row(id: Int, sex: Double, age: Double, height: Double, weight: Double, salary: Double)
        =>
          (id, Vectors.dense(id, sex, age, height, weight, salary))
      }).toDF("id", "features")

    // 正则化每个向量到1阶范数
    val pNorm: Normalizer = new Normalizer()
      .setInputCol("features")
      .setOutputCol("normed")
      .setP(1)

    val pNormedDF: DataFrame = pNorm.transform(df)
    pNormedDF.show(10, false)

    println("-------------------骚气的分割线---------------------")

    // 标准差规范化
    val standardNorm: StandardScaler = new StandardScaler()
      .setInputCol("features")
      .setOutputCol("standard")
      .setWithMean(true) // 0均值标准差算法

    val standarded: DataFrame = standardNorm.fit(df).transform(df)
    standarded.show(10, false)
    println("-------------------骚气的分割线---------------------")

    // minmaxscaler  最大最小值缩放
    val minMaxScaler: MinMaxScaler = new MinMaxScaler()
      .setInputCol("features")
      .setOutputCol("minmax")
    val minmaxed: DataFrame = minMaxScaler.fit(df).transform(df)
    minmaxed.show(10, false)

    println("-------------------骚气的分割线---------------------")
    // maxabsScaler  最大绝对值缩放
    val maxabs: MaxAbsScaler = new MaxAbsScaler()
      .setInputCol("features")
      .setOutputCol("maxabs")
    val maxabsed: DataFrame = maxabs.fit(df).transform(df)
    maxabsed.show(10, false)
    spark.close()
  }
}
