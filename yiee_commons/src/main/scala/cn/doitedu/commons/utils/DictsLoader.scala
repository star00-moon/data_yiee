package cn.doitedu.commons.utils

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}


/**
  * @author: 余辉
  * @blog: https://blog.csdn.net/silentwolfyh
  * @create: 2019/10/22
  * @description: 各类字典加载工具
  *
  *               步鄹：
  *               1、加载地域信息字典 loadAreaDict    "user_profile/data/areadict"
  *               2、加载idmapping映射字典  loadIdmpDict    "user_profile/data/output/idmp/day01"
  *               3、加载app信息字典  loadAppDict   "user_profile/data/appdict"
  *               4、加载url内容信息字典  loadUrlContentDict
  **/
object DictsLoader {

  /**
    * 加载地域信息字典
    *
    * @param spark sparksession
    * @param path  字典所在路径
    * @return 变成 kv 形式 Map(geo, (province, city, district))
    */
  def loadAreaDict(spark: SparkSession, path: String): collection.Map[String, (String, String, String)] = {
    val area: DataFrame = spark.read.parquet(path)
    area.rdd.map(row => {
      val geo: String = row.getAs[String]("geo")
      val province: String = row.getAs[String]("province")
      val city: String = row.getAs[String]("city")
      val district: String = row.getAs[String]("district")
      (geo, (province, city, district))
    }).collectAsMap()
  }

  /**
    * 加载idmapping映射字典
    *
    * @param spark
    * @param path
    * @return 变成 kv 形式 Map[id, gid]
    */
  def loadIdmpDict(spark: SparkSession, path: String): collection.Map[Long, Long] = {
    val idAndGids: DataFrame = spark.read.parquet(path)
    idAndGids.rdd.map(row => {
      val id: Long = row.getAs[Long]("id")
      val gid: Long = row.getAs[Long]("gid")
      (id, gid)
    }).collectAsMap()
  }


  /**
    * 加载app信息字典
    * 1、需要切分数据， ,appid->4, appname->1, appdesc->5
    *
    * @param spark
    * @param path
    * @return 变成 kv 形式 Map[appid, (appname, appdesc)]
    */
  def loadAppDict(spark: SparkSession, path: String): collection.Map[String, (String, String)] = {
    val appDs: Dataset[String] = spark.read.textFile(path)
    appDs
      .rdd
      .map(line => {
        val arr: Array[String] = line.split("\t", -1)
        val appid: String = arr(4)
        val appname: String = arr(1)
        val appdesc: String = arr(5)
        (appid, (appname, appdesc))
      })
      .filter(tp => StringUtils.isNotBlank(tp._1))
      .collectAsMap()
  }


  /**
    * 加载url内容信息字典
    * 1、需要切分数据， ,category->0   title->1    url->2
    *
    * @param spark
    * @param path
    * @return 变成 kv 形式 Map[url, (title, category)]
    */
  def loadUrlContentDict(spark: SparkSession, path: String): collection.Map[String, (String, String)] = {
    val urlContentDs: Dataset[String] = spark.read.textFile(path)
    urlContentDs
      .rdd
      .map(line => {
        val arr: Array[String] = line.split("\001", -1)
        val category: String = arr(0)
        val title: String = arr(1)
        val url: String = arr(2)
        (url, (title, category))
      })
      .filter(tp => StringUtils.isNotBlank(tp._1))
      .collectAsMap()
  }

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkUtil.getSparkSession(this.getClass.getTypeName)
    import spark.implicits._

    // 1、加载app信息字典
    //    val appMap: collection.Map[String, (String, String)] = loadAppDict(spark, "user_profile/data/appdict")
    //    val bcApp = spark.sparkContext.broadcast(appMap)

    // 2、加载地域信息字典
    //    val areaMap: collection.Map[String, (String, String, String)] = loadAreaDict(spark, "user_profile/data/areadict")
    //    val bcArea = spark.sparkContext.broadcast(areaMap)
    //    bcArea.value.foreach(println)

    // 3、加载idmp映射字典
    val idmpMap: collection.Map[Long, Long] = loadIdmpDict(spark, "user_profile/data/output/idmp/day01")
    val bcIdmap = spark.sparkContext.broadcast(idmpMap)
    bcIdmap.value.take(10).foreach(println)
  }
}
