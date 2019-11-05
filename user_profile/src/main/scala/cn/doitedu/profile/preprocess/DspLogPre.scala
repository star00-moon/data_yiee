package cn.doitedu.profile.preprocess

import ch.hsr.geohash.GeoHash
import cn.doitedu.commons.utils.SparkUtil
import cn.doitedu.commons.utils.DictsLoader._
import org.apache.commons.lang3.{StringEscapeUtils, StringUtils}

/**
 * @author: 余辉
 * @blog:   https://blog.csdn.net/silentwolfyh
 * @create: 2019/10/22
 * @description: DSP竞价日志预处理 ，参考 4.5.2.	DSP数据字段说明
 **/
object DspLogPre {

  def main(args: Array[String]): Unit = {

    // 1、建立session连接
    val spark = SparkUtil.getSparkSession(this.getClass.getSimpleName)
    import spark.implicits._

    // 2、加载原始数据
    val dsplog = spark.read.textFile("user_profile/data/dsplog/day01")

    // 3、加载app信息字典
    val appMap = loadAppDict(spark, "user_profile/data/appdict")
    val bcApp = spark.sparkContext.broadcast(appMap)

    // 4、加载地域信息字典
    val areaMap = loadAreaDict(spark, "user_profile/data/areadict")
    val bcArea = spark.sparkContext.broadcast(areaMap)

    // 5、加载idmp映射字典
    val idmpMap = loadIdmpDict(spark, "user_profile/data/output/idmp/day01")
    val bcIdmap = spark.sparkContext.broadcast(idmpMap)

    // 6、切分字段
    dsplog
      .rdd
      .map(line => {
        line.split(",", -1)
      })
      .filter(arr => {
        // 6-1、过滤字段长度为85 和 第46 到 50为空字符串的数据(.mkString("").replaceAll("null", "").trim)
        val flag1 = arr.size == 85

        val ids = for (i <- 46 to 50) yield arr(i)
        val idsStr = ids.mkString("").replaceAll("null", "").trim

        val flag2 = !idsStr.equals("")

        flag1 && flag2
      })
      // 6-2、数据集成，封装bean
      .map(arr => DspLogBean.genDspLogBean(arr))
      .map(bean => {

        // 6-3、从广播变量中取出3个字典
        val appDict = bcApp.value
        val areaDict = bcArea.value
        val idmpDict = bcIdmap.value

        // 6-4、定义好要追加集成的字段,[provincename,cityname,district,appname,appDesc,gid="-1"]
        var province = bean.provincename
        var city = bean.cityname
        var district = bean.district
        var appname = bean.appname
        var appdesc = bean.appDesc
        var gid = "-1"

        // 6-5、取出gps坐标，并集成省市区信息  (lng > 73 && lng < 140 && lat > 3 && lat < 54)
        // 6-5-1、lng：经度 ， lat 纬度
        val lng: Double = bean.lng
        val lat: Double = bean.lat
        if (lng > 73 && lng < 140 && lat > 3 && lat < 54) {
          // 6-5-2、使用GeoHash.withCharacterPrecision获取5长度的标识
          val geo: String = GeoHash.withCharacterPrecision(lat, lng, 5).toBase32
          // 6-5-3、从地域字段中提取值，如果存在则赋值给 province city district
          val areaOption = areaDict.get(geo)
          if (areaOption.isDefined) {
            val area = areaOption.get
            province = area._1
            city = area._2
            district = area._3
          }
        }

        // 6-6、取出appid，并集成app信息  user_profile/data/appdict/app_dict.txt
        val appOption = appDict.get(bean.appid)
        if (appOption.isDefined) {
          val appInfo = appOption.get
          appname = appInfo._1
          appdesc = appInfo._2
        }


        // 6-7、取出5个id标识，并集成gid  ；Array(imei, idfa, udid, andid, mac)
        val imei = bean.imei
        val idfa = bean.idfa
        val udid = bean.openudid
        val andid = bean.androidid
        val mac = bean.mac
        val id = Array(imei, idfa, udid, andid, mac).filter(StringUtils.isNotBlank(_)).head
        gid = idmpDict.getOrElse(id.hashCode.toLong, -1L).toString

        // 6-8、返回追加完字段的bean
        bean.provincename = province
        bean.cityname = city
        bean.district = district
        bean.appname = appname
        bean.appDesc = appdesc
        bean.gid = gid

        bean
      })

      // 7、保存结果
      .toDF()
      .coalesce(1)
      .write
      .parquet("user_profile/data/output/dsplog/day01")
    spark.close()
  }
}
