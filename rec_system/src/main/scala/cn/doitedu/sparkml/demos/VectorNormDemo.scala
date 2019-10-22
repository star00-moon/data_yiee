package cn.doitedu.sparkml.demos

import cn.doitedu.commons.utils.SparkUtil
import org.apache.spark.ml.feature.{MaxAbsScaler, MinMaxScaler, Normalizer, StandardScaler}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.Row

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
    val spark = SparkUtil.getSparkSession(this.getClass.getSimpleName)
    import spark.implicits._

    // 将数据向量化
    val df = spark.read
      .option("header", true)
      .option("inferSchema", true)
      .csv("rec_system/demodata/vecdata/vec.csv")
      .map({
        case Row(id: Int, sex: Double, age: Double, height: Double, weight: Double, salary: Double)
        =>
          (id, Vectors.dense(id, sex, age, height, weight, salary))
      }).toDF("id", "features")

    // 构造p范数向量规范化器
    val pNorm = new Normalizer()
      .setInputCol("features")
      .setOutputCol("normed")
      .setP(1)

    val pNormedDF = pNorm.transform(df)
    pNormedDF.show(10, false)

    println("-------------------骚气的分割线---------------------")

    // 标准差规范化
    val standardNorm = new StandardScaler()
      .setInputCol("features")
      .setOutputCol("standard")
      .setWithMean(true) // 0均值标准差算法

    val standarded = standardNorm.fit(df).transform(df)
    standarded.show(10, false)
    println("-------------------骚气的分割线---------------------")

    // minmaxscaler  最大最小值缩放
    val minMaxScaler = new MinMaxScaler().setInputCol("features").setOutputCol("minmax")
    val minmaxed = minMaxScaler.fit(df).transform(df)
    minmaxed.show(10, false)

    println("-------------------骚气的分割线---------------------")
    // maxabsScaler  最大绝对值缩放
    val maxabs = new MaxAbsScaler().setInputCol("features").setOutputCol("maxabs")
    val maxabsed = maxabs.fit(df).transform(df)
    maxabsed.show(10, false)
    spark.close()
  }
}
