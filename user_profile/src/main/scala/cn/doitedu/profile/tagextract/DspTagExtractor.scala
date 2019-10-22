package cn.doitedu.profile.tagextract

import java.util

import cn.doitedu.commons.utils.SparkUtil
import com.hankcs.hanlp.HanLP
import com.hankcs.hanlp.seg.common.Term
import org.apache.commons.lang3.StringUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.mutable.ListBuffer

/**
  * @author: 余辉
  * @blog: https://blog.csdn.net/silentwolfyh
  * @create: 2019/10/19
  * @description:
  * 1、dsp竞价日志数据标签抽取程序
  * 2、返回：gid，模块，便签，值，权重 (Long, String, String, String, Double)
  **/
object DspTagExtractor {
  def extractDspTags(spark: SparkSession, path: String) = {

    import spark.implicits._

    // 1、加载预处理好的dsp竞价日志
    val df: DataFrame = spark.read.parquet(path)

    // 2、提取所需要的字段
    val data: DataFrame = df.select(
      "gid",
      "appname",
      "appDesc",
      "biz",
      "title",
      "imei",
      "mac",
      "idfa",
      "openudid",
      "androidid",
      "provincename",
      "cityname",
      "district",
      "device",
      "adspacetypename",
      "networkmannername",
      "ispname",
      "client"
    )

    // 3、抽取标签
    val dspTags = data.rdd.flatMap({
      case Row(
      gidStr: String,
      appname: String,
      appDesc: String,
      biz: String,
      title: String,
      imei: String,
      mac: String,
      idfa: String,
      openudid: String,
      androidid: String,
      provincename: String,
      cityname: String,
      district: String,
      device: String,
      adspacetypename: String,
      networkmannername: String,
      ispname: String,
      client: Int
      ) => {

        // 3-1 建立一个ListBuffer 存储数据
        val lst = new ListBuffer[(Long, String, String, String, Double)]()

        // 3-2、全局统一标识id gid
        val gid = gidStr.toLong

        // 3-3、标识appname标签,包括：phone、imei、imsi、manufacture 权重为 1.0
        if (StringUtils.isNoneBlank(appname))
          lst += ((gid, "M012", "T121", appname, 1.0))

        // 3-4、标识aappDesc标签,需要 HanLP.segment 进行切分 ,过滤单字词（还可以过滤停止<和谐>词）
        val terms: util.List[Term] = HanLP.segment(appDesc)
        import scala.collection.JavaConversions._
        if (terms.size() > 0) {
          val kwTagLst = terms.map(term => term.word).filter(_.size > 1).map(word => (gid, "M012", "T123", word, 1.0))
          lst.++=(kwTagLst)
        }

        //3-5、标识 provincename 标签,
        if (StringUtils.isNotBlank(provincename))
          lst += ((gid, "M014", "T141", provincename, 1.0))

        // 3-6、标识 cityname 城市标签
        if (StringUtils.isNotBlank(cityname))
          lst += ((gid, "M014", "T142", cityname, 1.0))

        // 3-7、标识 district 设备所在县名称标签
        if (StringUtils.isNotBlank(district))
          lst += ((gid, "M014", "T143", district, 1.0))

        // 3-8、标识 biz 标签
        if (StringUtils.isNotBlank(biz))
          lst += ((gid, "M014", "T144", biz, 1.0))

        // 3-9、标识 title ,需要 HanLP.segment 进行切分 ,过滤单字词（还可以过滤停止<和谐>词）
        val terms2 = HanLP.segment(title)
        if (terms2.size() > 0) {
          val kwTagLst = terms2.map(term => term.word).filter(_.size > 1).map(word => (gid, "M013", "T131", word, 1.0))
          lst.++=(kwTagLst)
        }

        // 3-10、处理id标识标签 包括：imei、idfa、mac、openudid、androidid
        lst += ((gid, "M000", "T001", imei, 1.0))
        lst += ((gid, "M000", "T002", idfa, 1.0))
        lst += ((gid, "M000", "T003", mac, 1.0))
        lst += ((gid, "M000", "T004", openudid, 1.0))
        lst += ((gid, "M000", "T005", androidid, 1.0))


        // 3-11、处理 client 终端属性标签 ， client  1 => "android"， 2 => "ios"， 3 => "wp" ，_ => "未知"
        val osname = client match {
          case 1 => "android"
          case 2 => "ios"
          case 3 => "wp"
          case _ => "未知"
        }

        lst += ((gid, "M003", "T301", device, 1.0))
        lst += ((gid, "M003", "T302", osname, 1.0))
        lst += ((gid, "M003", "T303", networkmannername, 1.0))
        lst += ((gid, "M003", "T304", ispname, 1.0))

        // 3-12、处理 adspacetypename 广告位属性标签
        lst += ((gid, "M011", "T111", adspacetypename, 1.0))

        lst
      }
    })
    dspTags
  }

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkUtil.getSparkSession(this.getClass.getSimpleName)
    import spark.implicits._
    val dspTags: RDD[(Long, String, String, String, Double)] = DspTagExtractor.extractDspTags(spark, "user_profile/data/output/dsplog/day01")
    dspTags.foreach(print)
  }
}
