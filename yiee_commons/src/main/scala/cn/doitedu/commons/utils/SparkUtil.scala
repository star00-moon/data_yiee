package cn.doitedu.commons.utils

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * @date: 2019/8/25
 * @site: www.doitedu.cn
 * @author: hunter.d 涛哥
 * @qq: 657270652
 * @description:
 */
object SparkUtil {

  /**
    * sparksession构建工具
    * @param appName
    * @param master
    * @param cnfMap
    * @return
    */
  def getSparkSession(appName:String = "app",master:String = "local[*]",cnfMap:Map[String,String] = Map.empty[String,String] ):SparkSession = {

    val conf = new SparkConf()
    conf.setAll(cnfMap)

    SparkSession.builder()
      .config(conf)
      .appName(appName)
      .master(master)
      .getOrCreate()
  }

}
