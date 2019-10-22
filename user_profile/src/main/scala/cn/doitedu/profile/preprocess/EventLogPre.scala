package cn.doitedu.profile.preprocess

import java.util

import ch.hsr.geohash.GeoHash
import cn.doitedu.commons.utils.{EventJson2Bean, SparkUtil}
import cn.doitedu.commons.utils.DictsLoader._
import com.hankcs.hanlp.HanLP
import com.hankcs.hanlp.seg.common.Term
import org.apache.commons.lang3.StringUtils


/**
  * @date: 2019/9/16
  * @site: www.doitedu.cn
  * @author: hunter.d 涛哥
  * @qq: 657270652
  * @description: 公司商城系统用户行为事件日志预处理
  */
object EventLogPre {

  def main(args: Array[String]): Unit = {

    // 1、建立session连接  import spark.implicits._
    val spark = SparkUtil.getSparkSession(this.getClass.getSimpleName)
    import spark.implicits._

    // 2、加载事件日志文件
    val ds = spark.read.textFile("user_profile/data/eventlog/day01")

    // 3、加载地域字典
    val area = loadAreaDict(spark, "user_profile/data/areadict")
    val bc_area = spark.sparkContext.broadcast(area)

    // 4、加载idmp字典
    val idmp = loadIdmpDict(spark, "user_profile/data/output/idmp/day01")
    val bc_idmp = spark.sparkContext.broadcast(idmp)

    // 5、字段转换
    val result = ds
      .map(_.split(" --> "))
      .filter(_.size > 1)
      .map(arr => EventJson2Bean.genBean(arr(1)))
      .filter(_ != null)
      .map(bean => {
        val areaDict = bc_area.value
        val idmpDict = bc_idmp.value

        var province = ""
        var city = ""
        var district = ""
        var gid = "-1"
        var title_kwds = ""

        // 5-1、集成省市区信息
        val lat = bean.latitude
        val lng = bean.longtitude
        if (lng > 73 && lng < 140 && lat > 3 && lat < 54) {
          val geo: String = GeoHash.withCharacterPrecision(lat, lng, 5).toBase32
          val areaOption = areaDict.get(geo)
          if (areaOption.isDefined) {
            val area = areaOption.get
            province = area._1
            city = area._2
            district = area._3
          }
        }

        // 5-2、集成gid  取出5个id标识，并集成gid
        val imei = bean.imei
        val account = bean.account
        val cookieid = bean.cookieid
        //val deviceId = bean.deviceId
        val androidId = bean.androidId
        val id = Array(imei, account, cookieid, androidId).filter(StringUtils.isNotBlank(_)).head

        gid = idmpDict.getOrElse(id.hashCode.toLong, -1L).toString

        // 5-3、对bean进行字段追加赋值
        bean.province = province
        bean.city = city
        bean.district = district
        bean.gid = gid


        // 5-4、抽取页面标题，进行分词  我有一头小毛驴我从来也不骑，有一天我心血来潮骑着去赶集  技术和服务
        // 比较流行的中文分词包有： IKAnalyzer（ik分词器）  庖丁分词   HanLp（不光做分词，还可以做各种NLP处理）
        val titleOption = bean.event.get("title")
        if (titleOption.isDefined) {
          val title = titleOption.get
          val terms: util.List[Term] = HanLP.segment(title)
          import scala.collection.JavaConversions._
          title_kwds = terms.map(term => term.word).filter(_.size > 1).mkString(" ")
        }
        bean.title_kwds = title_kwds

        bean
      })

    // 6、结果输出
    result.coalesce(1).write.parquet("user_profile/data/output/eventlog/day01")

    spark.close()
  }
}
