package cn.doitedu.commons.utils

/**
  * @author: 余辉
  * @create: 2019-10-16 10:30
  * @description: Parquet 格式文件查看
  **/

import org.apache.spark.sql.{DataFrame, SparkSession}

object ShowFiles {
  def main(args: Array[String]): Unit = {

    // 1、建立 SparkSession
    val spark: SparkSession = SparkUtil.getSparkSession()

    // 2、设置路径和读取 parquet 格式
    val path = "user_profile/demodata/graphx/out_gidlog";

    //    val df: DataFrame = showParquet(spark,path)
    val df: DataFrame = showCsv(spark, "user_profile/demodata/graphx/out_gidlog")

    // 3、打印Schema 和 show 100
    df.printSchema()
    df.show(100, false)

    // 4、关闭Spark
    spark.close()
  }

  def showParquet(spark: SparkSession, path: String): DataFrame = {
    spark.read.parquet(path) /*.where("eventType='ad_show'")*/
  }

  def showCsv(spark: SparkSession, path: String): DataFrame = {
    spark.read.csv(path)
  }
}