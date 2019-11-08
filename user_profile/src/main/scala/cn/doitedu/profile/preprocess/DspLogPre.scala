package cn.doitedu.profile.preprocess

import ch.hsr.geohash.GeoHash
import cn.doitedu.commons.utils.SparkUtil
import cn.doitedu.commons.utils.DictsLoader._
import org.apache.commons.lang3.{StringEscapeUtils, StringUtils}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.collection.immutable

/**
  * @author: 余辉
  * @blog: https://blog.csdn.net/silentwolfyh
  * @create: 2019/10/22
  * @description: DSP竞价日志预处理 ，参考 4.5.2.	DSP数据字段说明
  **/
object DspLogPre {

  def main(args: Array[String]): Unit = {

    // 1、建立session连接
    val spark: SparkSession = SparkUtil.getSparkSession(this.getClass.getSimpleName)
    import spark.implicits._

    // 2、加载原始数据
    val dsplog: Dataset[String] = spark.read.textFile("user_profile/data/dsplog/day01")

    // 3、加载app信息字典,放入广播变量 bcApp
    val appMap: collection.Map[String, (String, String)] = loadAppDict(spark, "user_profile/data/appdict")
    val bcApp: Broadcast[collection.Map[String, (String, String)]] = spark.sparkContext.broadcast(appMap)

    // 4、加载地域信息字典,放入广播变量 bcArea
    val areaMap: collection.Map[String, (String, String, String)] = loadAreaDict(spark, "user_profile/data/areadict")
    val bcArea: Broadcast[collection.Map[String, (String, String, String)]] = spark.sparkContext.broadcast(areaMap)

    // 5、加载idmp映射字典,放入广播变量 bcIdmap
    val idmpMap: collection.Map[Long, Long] = loadIdmpDict(spark, "user_profile/data/output/idmp/day01")
    val bcIdmap: Broadcast[collection.Map[Long, Long]] = spark.sparkContext.broadcast(idmpMap)

    // 6、dsplog数据处理过程
    dsplog
      .rdd
      .map(line => {
        // 6-1、通过map函数，切分字段 ","
        line.split(",", -1)
      })
      .filter(arr => {
      // 6-1、filter：判断字段长度是否为85
      val flag1: Boolean = arr.length == 85

      // 6-2、filter：判断第46 到 50为空字符串的数据不为空 yield arr(i) ，(.mkString("").replaceAll("null", "").trim)
      val ids: immutable.IndexedSeq[String] = for (i <- 46 to 50) yield arr(i)
      val idsStr: String = ids.mkString("").replaceAll("null", "").trim
      val flag2: Boolean = !idsStr.equals("")

      // 6-3、true && true ==》true
      flag1 && flag2
    }).map(arr => {
      // 6-4、通过map函数，将数据封装bean
      DspLogBean.genDspLogBean(arr)
    }).map(bean => {
      // 6-5、从广播变量中取出3个字典
      val appDict: collection.Map[String, (String, String)] = bcApp.value
      val areaDict: collection.Map[String, (String, String, String)] = bcArea.value
      val idmpDict: collection.Map[Long, Long] = bcIdmap.value

      // 6-6、定义好要追加集成的字段,[provincename,cityname,district,appname,appDesc,gid="-1"]
      var province: String = bean.provincename
      var city: String = bean.cityname
      var district: String = bean.district
      var appname: String = bean.appname
      var appdesc: String = bean.appDesc
      var gid = "-1"

      // 6-7、取出gps坐标，并集成省市区信息  (lng > 73 && lng < 140 && lat > 3 && lat < 54)
      // 6-7-1、lng：经度 ， lat 纬度
      val lng: Double = bean.lng
      val lat: Double = bean.lat

      // 6-7-2、使用GeoHash.withCharacterPrecision获取5长度的标识
      if (lng > 73 && lng < 140 && lat > 3 && lat < 54) {
        val geo: String = GeoHash.withCharacterPrecision(lat, lng, 5).toBase32

        // 6-7-3、从地域字段中提取值，如果存在则赋值给 province city district
        val areaOption: Option[(String, String, String)] = areaDict.get(geo)
        if (areaOption.isDefined) {
          val area: (String, String, String) = areaOption.get
          province = area._1
          city = area._2
          district = area._3
        }
      }
      // 6-8、取出appid，并集成app信息
      val appOption: Option[(String, String)] = appDict.get(bean.appid)
      if (appOption.isDefined) {
        val appInfo: (String, String) = appOption.get
        appname = appInfo._1
        appdesc = appInfo._2
      }

      // 6-9、取出5个id标识，并集成gid
      // 6-9-1 过滤空值之后取id ， Array(imei, idfa, openudid, androidid, mac)
      val imei: String = bean.imei
      val idfa: String = bean.idfa
      val udid: String = bean.openudid
      val andid: String = bean.androidid
      val mac: String = bean.mac
      val id: String = Array(imei, idfa, udid, andid, mac).filter(StringUtils.isNotBlank(_)).head
      // 6-9-2 idmpMap中取gid，如果没有则为 -1
      gid = idmpDict.getOrElse(id.hashCode.toLong, -1L).toString

      // 6-10、返回追加完字段的bean [provincename,cityname,district,appname,appDesc,gid="-1"]
      bean.provincename = province
      bean.cityname = city
      bean.district = district
      bean.appname = appname
      bean.appDesc = appdesc
      bean.gid = gid

      bean
    })

      // 7、保存结果为 parquet 格式
      .toDF()
      .coalesce(1)
      .write
      .parquet("user_profile/data/output/dsplog/day01")
    spark.close()
  }
}
