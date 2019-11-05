package cn.doitedu.profile.preprocess

import java.io.File
import java.util

import ch.hsr.geohash.GeoHash
import cn.doitedu.commons.beans.EventLogBean
import cn.doitedu.commons.utils.{EventJson2Bean, FileUtils, SparkUtil}
import cn.doitedu.commons.utils.DictsLoader._
import com.hankcs.hanlp.HanLP
import com.hankcs.hanlp.seg.common.Term
import org.apache.commons.lang3.StringUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{Dataset, SparkSession}


/**
  * @author: 余辉
  * @blog: https://blog.csdn.net/silentwolfyh
  * @create: 2019/10/22
  * @description: 公司商城系统用户行为事件日志预处理
  **/
object EventLogPre {

  def main(args: Array[String]): Unit = {

    // 1、建立session连接  import spark.implicits._
    val spark: SparkSession = SparkUtil.getSparkSession(this.getClass.getSimpleName)
    import spark.implicits._

    // 2、加载事件日志文件
    val ds: Dataset[String] = spark.read.textFile("user_profile/data/eventlog/day01")

    // 3、加载地域字典
    val area: collection.Map[String, (String, String, String)] = loadAreaDict(spark, "user_profile/data/areadict")
    val bc_area: Broadcast[collection.Map[String, (String, String, String)]] = spark.sparkContext.broadcast(area)

    // 4、加载idmp字典
    val idmp: collection.Map[Long, Long] = loadIdmpDict(spark, "user_profile/data/output/idmp/day01")
    val bc_idmp: Broadcast[collection.Map[Long, Long]] = spark.sparkContext.broadcast(idmp)

    // 5、字段转换。1)按照 " --> " 切分; 2）过滤 _.size > 1 ; 3）转为EventJson2Bean对象 ; 4）过滤空值
    val result: Dataset[EventLogBean] = ds
      .map(_.split(" --> "))
      .filter(_.size > 1)
      .map(arr => EventJson2Bean.genBean(arr(1)))
      .filter(_ != null)
      .map(bean => {
        // 5-1、bean对象处理，取出广播变量
        val areaDict: collection.Map[String, (String, String, String)] = bc_area.value
        val idmpDict: collection.Map[Long, Long] = bc_idmp.value

        // 5-2、设置字符串【province，city，district，title_kwds 为空】且 gid = "-1"
        var province = ""
        var city = ""
        var district = ""
        var title_kwds = ""
        var gid = "-1"

        // 5-3、通过经度纬度集成省市区信息
        val lat: Double = bean.latitude
        val lng: Double = bean.longtitude
        if (lng > 73 && lng < 140 && lat > 3 && lat < 54) {
          val geo: String = GeoHash.withCharacterPrecision(lat, lng, 5).toBase32
          val areaOption: Option[(String, String, String)] = areaDict.get(geo)
          if (areaOption.isDefined) {
            val area: (String, String, String) = areaOption.get
            province = area._1
            city = area._2
            district = area._3
          }
        }

        // 5-4、通过  Array(imei, account, cookieid, androidId) 集成gid
        val imei: String = bean.imei
        val account: String = bean.account
        val cookieid: String = bean.cookieid
        val androidId: String = bean.androidId
        val id: String = Array(imei, account, cookieid, androidId).filter(StringUtils.isNotBlank(_)).head

        // 5-5、通过idmpDict找到 gid，如果没有则为 -1L，且变成字符串类型
        gid = idmpDict.getOrElse(id.hashCode.toLong, -1L).toString

        // 5-6、对bean进行字段追加赋值【province，city，district,gid】
        bean.province = province
        bean.city = city
        bean.district = district
        bean.gid = gid

        /** *
          * 5-7、抽取页面标题 title，进行分词  我有一头小毛驴我从来也不骑，有一天我心血来潮骑着去赶集  技术和服务
          * 比较流行的中文分词包有： IKAnalyzer（ik分词器）  庖丁分词   HanLp（不光做分词，还可以做各种NLP处理）
          */
        val titleOption: Option[String] = bean.event.get("title")

        // 5-8、标题有值则通过分词进行处理，滤单个字，且.mkString(" ")
        if (titleOption.isDefined) {
          val title: String = titleOption.get
          val terms: util.List[Term] = HanLP.segment(title)
          import scala.collection.JavaConversions._
          title_kwds = terms.map(term => term.word).filter(_.size > 1).mkString(" ")
        }
        bean.title_kwds = title_kwds

        // 5-9、按照bean对象返回
        bean
      })

    // 6、结果输出
    FileUtils.deleteDir(new File("user_profile/data/output/eventlog/day01"))
    result.coalesce(1).write.parquet("user_profile/data/output/eventlog/day01")

    // 7、spark关闭
    spark.close()
  }
}
