package cn.doitedu.commons.utils

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.SparkSession


/**
 * @date: 2019/9/16
 * @site: www.doitedu.cn
 * @author: hunter.d 涛哥
 * @qq: 657270652
 * @description:  各类字典加载工具
  *
  *  步鄹：
  *  1、加载地域信息字典 loadAreaDict    "user_profile/data/areadict"
  *  2、加载idmapping映射字典  loadIdmpDict    "user_profile/data/output/idmp/day01"
  *  3、加载app信息字典  loadAppDict   "user_profile/data/appdict"
  *  4、加载url内容信息字典  loadUrlContentDict
 */
object DictsLoader {


  /**
    * 加载地域信息字典  geo province  city  district
    * @param spark  sparksession
    * @param path 字典所在路径
    * @return
    */
  def loadAreaDict(spark:SparkSession,path:String):collection.Map[String, (String, String,String)]={

    val areaDF = spark.read.parquet(path)
    val areaMap: collection.Map[String, (String, String, String)] = areaDF
      .rdd
      .map(row => {
        val geo = row.getAs[String]("geo")
        val province = row.getAs[String]("province")
        val city = row.getAs[String]("city")
        val district = row.getAs[String]("district")
        (geo, (province, city, district))
      })
      .collectAsMap()
    areaMap
  }


  /**
    * 加载idmapping映射字典 id, gid
    * @param spark
    * @param path
    * @return
    */
  def loadIdmpDict(spark:SparkSession,path:String):collection.Map[Long, Long] = {
    val idmpDF = spark.read.parquet(path)
    val idmpMap: collection.Map[Long, Long] = idmpDF
      .rdd
      .map(row => {

        val id = row.getAs[Long]("id")
        val gid = row.getAs[Long]("gid")
        (id, gid)
      })
      .collectAsMap()

    idmpMap
  }


  /**
    * 加载app信息字典
    * @param spark
    * @param path
    * @return
    */
  def loadAppDict(spark:SparkSession,path:String):collection.Map[String, (String, String)]={
    val appDs = spark.read.textFile(path)
    val appMap: collection.Map[String, (String, String)] = appDs
      .rdd
      .map(line => {

        val arr = line.split("\t", -1)
        val appid = arr(4)
        val appname = arr(1)
        val appdesc = arr(5)

        // 变成 kv 形式
        (appid, (appname, appdesc))
      })
      .filter(tp => StringUtils.isNotBlank(tp._1))
      .collectAsMap()

    appMap
  }



  /**
    * 加载url内容信息字典
    * @param spark
    * @param path
    * @return
    */
  def loadUrlContentDict(spark:SparkSession,path:String):collection.Map[String, (String, String)]={
    val urlContentDs = spark.read.textFile(path)
    val urlContentMap: collection.Map[String, (String, String)] = urlContentDs
      .rdd
      .map(line => {

        val arr = line.split("\001", -1)
        val category = arr(0)
        val title = arr(1)
        val url = arr(2)

        // 变成 kv 形式
        (url, (title, category))
      })
      .filter(tp => StringUtils.isNotBlank(tp._1))
      .collectAsMap()

    urlContentMap
  }


}
