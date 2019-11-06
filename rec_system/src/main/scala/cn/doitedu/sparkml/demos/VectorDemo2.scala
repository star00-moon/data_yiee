package cn.doitedu.sparkml.demos

import cn.doitedu.commons.utils.SparkUtil
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * @author: 余辉
  * @blog: https://blog.csdn.net/silentwolfyh
  * @create: 2019/10/22
  * @description:
  * 1、两点距离公式
  * 2、hive函数大全：https://yuhui.blog.csdn.net/article/details/102932531
  **/
object VectorDemo2 {

  def main(args: Array[String]): Unit = {

    // 1、建立session连接
    val spark: SparkSession = SparkUtil.getSparkSession(this.getClass.getSimpleName)

    // 2、读取数据转为DF，且创建临时view
    val df: DataFrame = spark.read.option("header", true).csv("rec_system/demodata/vecdata/vec.csv")
    df.createTempView("df")

    /** *
      * 1、两张表相连
      * 2、左边id小于右边id （1--2,3,4,5，6）（2--3,4,5,6,）
      * 3、pow(double d,double p)--d的p次幂，返回double型；
      * 4、1doc/两点距离公式.png
      */
    spark.sql(
      """
        |
        |select
        |a.id as aid,
        |b.id as bid,
        |pow(
        |   pow(a.sex - b.sex,2.0)+
        |   pow(a.age - b.age,2.0)+
        |   pow(a.height - b.height,2.0)+
        |   pow(a.weight - b.weight,2.0)+
        |   pow(a.salary - b.salary,2.0)
        |,0.5) as eudist
        |from df a join df b
        |on a.id<b.id
        |
      """.stripMargin)
      .show(20, false)
    spark.close()
  }
}
