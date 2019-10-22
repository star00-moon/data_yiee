package cn.doitedu.course.dw.pre

import ch.hsr.geohash.GeoHash
import cn.doitedu.commons.beans.EventLogBean
import cn.doitedu.commons.utils.{EventJson2Bean, SparkUtil}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.{Dataset, Row}


/**
  * @date: 2019/8/27
  * @site: www.doitedu.cn
  * @author: hunter.d 涛哥
  * @qq: 657270652
  * @description: 流量日志预处理程序
  */
object EventLogPreprocess {

  def main(args: Array[String]): Unit = {

    if (args.size < 4) {
      println(
        """
          |
          |Usage:
          |args(0):地理位置字典路径
          |args(1):待处理的日志文件路径
          |args(2):解释失败的gps坐标输出路径
          |args(3):预处理完成的日志数据输出路径
          |
        """.stripMargin)
      sys.exit(1)
    }


    var master = "local[*]"
    if(args(4).equals("yarn")) master = "yarn"
    val spark = SparkUtil.getSparkSession(this.getClass.getSimpleName,master)
    import spark.implicits._
    // 从 "元数据库” 中加载地理位置字典
    /*val props = new Properties()
    props.setProperty("user","root")
    props.setProperty("password","root")
    val dictdf = spark.read.jdbc("jdbc:mysql://localhost:3306/demo","t_area_dict",props)*/

    // 或者从“文件”中加载地理位置字典
    val dictDF = spark.read.parquet(args(0))

    // 通过广播变量，发到日志处理的 task 端，关联时不需要shuffle（map端join）
    val areaMap: collection.Map[String, Array[String]] = dictDF.map({
      case Row(geo: String, province: String, city: String, district: String)
      => {
        (geo, Array(province, city, district))
      }
    }).rdd.collectAsMap()

    val bc = spark.sparkContext.broadcast(areaMap)

    // 加载日志数据
    // ds和df又有何区别？ 和rdd又有何区别联系？
    val ds = spark.read.textFile(args(1))

    // 解析json并按规则过滤
    val result: Dataset[EventLogBean] = ds
      .map(_.split(" --> "))
      .filter(_.size > 1) // 少用if，多用filter
      .map(arr => { // 将json解析成logbean
      val json = arr(1)
      EventJson2Bean.genBean(json) //封装好的方法将json变成bean
    })
      .filter(bean => { // 过滤掉解析失败的，和缺失字段的
        val flag = (bean != null)
        val sb = new StringBuilder
        val ids = sb.append(bean.account)
          .append(bean.androidId)
          .append(bean.cookieid)
          .append(bean.deviceId)
          .append(bean.imei)
          .toString()
          .replaceAll("null", "")
        flag && StringUtils.isNotBlank(ids) && bean.event != null
      })
      // 集成省市区信息
      .map(bean => { // 为每条日志集成省市区信息
      val lat = bean.latitude
      val lng = bean.longtitude
      val geo = GeoHash.withCharacterPrecision(lat, lng, 5).toBase32

      val areaDict: collection.Map[String, Array[String]] = bc.value

      val areaInfo: Array[String] = areaDict.getOrElse(geo, Array("", "", ""))

      bean.province = areaInfo(0)
      bean.city = areaInfo(1)
      bean.district = areaInfo(2)

      bean
    })


    // 将result中没有省市区的 gps坐标挑出来保存，后续可以用高德api去解析，并丰富咱们的地理位置字典库
    val toParseGps = result.filter(b => StringUtils.isBlank(b.province)).map(b => (b.longtitude, b.latitude)).toDF("lng", "lat")
    toParseGps.coalesce(1).write.parquet(args(2))


    // 保存日志的处理结果
    result.coalesce(1).write.parquet(args(3))

    spark.close()
  }

}