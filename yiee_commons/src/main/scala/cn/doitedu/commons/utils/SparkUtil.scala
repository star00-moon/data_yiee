package cn.doitedu.commons.utils

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * @author: 余辉
 * @blog:   https://blog.csdn.net/silentwolfyh
 * @create: 2019/10/22
 * @description: sparksession构建工具
 **/
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
