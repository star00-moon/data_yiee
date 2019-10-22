package cn.doitedu.sparkml.demos

import cn.doitedu.commons.utils.SparkUtil

/**
 * @date: 2019/9/24
 * @site: www.doitedu.cn
 * @author: hunter.d 涛哥
 * @qq: 657270652
 * @description:
  *
 */
object VectorDemo2 {

  def main(args: Array[String]): Unit = {

    val spark = SparkUtil.getSparkSession(this.getClass.getSimpleName)

    val df = spark.read.option("header",true).csv("rec_system/demodata/vecdata/vec.csv")
    df.createTempView("df")

    // pow(double d,double p)--d的p次幂，返回double型；
    spark.sql(
      """
        |
        |select
        |a.id as aid,
        |b.id as bid,
        |pow(pow(a.sex-b.sex,2.0)+pow(a.age - b.age,2.0)+pow(a.height - b.height,2.0)+pow(a.weight - b.weight,2.0)+pow(a.salary - b.salary,2.0),0.5) as eudist
        |
        |from df a join df b
        |on a.id<b.id
        |
      """.stripMargin)
        .show(20,false)
    spark.close()
  }
}
