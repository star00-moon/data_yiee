package cn.doitedu.profile.tagextract

import cn.doitedu.commons.beans.EventLogBean
import cn.doitedu.commons.utils.SparkUtil
import com.hankcs.hanlp.HanLP
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.mutable.ListBuffer


/**
  * @author: 余辉 https://blog.csdn.net/silentwolfyh
  * @create: 2019/10/18
  * @description:
  * 1、商城系统用户行为日志标签抽取
  * 2、返回：gid，模块，便签，值，权重 (Long, String, String, String, Double)
  **/
object EventLogTagExtractor {

  def extractEventLogTags(spark: SparkSession, path: String): RDD[(Long, String, String, String, Double)] = {
    // 1、加载商品信息表,变成Map集合,放入广播地址
    val goodsDF = spark.read.option("header", "true").csv("user_profile/data/t_user_goods")
    val goodsMap = goodsDF.rdd.map({
      case Row(skuid: String, pname: String, brand: String, c1: String, c2: String, c3: String)
      => (skuid, Array(pname, brand, c1, c2, c3))
    }).collectAsMap()
    val bc = spark.sparkContext.broadcast(goodsMap)

    // 2、来源数据转为Bean，as[EventLogBean]  import spark.implicits._
    import spark.implicits._
    val ds = spark.read.parquet(path).as[EventLogBean]

    // 3、数据映射成标签数据，gid，模块，便签，值，权重
    ds.rdd.flatMap(bean => {
      // 3-1 建立一个ListBuffer 存储数据
      val lst = new ListBuffer[(Long, String, String, String, Double)]

      // 3-1、全局统一标识id gid
      val gid = bean.gid.toLong

      // 3-2、标识标签模块,包括：imei、account、androidId，权重为 1.0
      val account = bean.account // 用户的标识之一
      val imei = bean.imei // 用户的标识之一
      val androidId = bean.androidId // 安卓系统的软id  用户的标识之一

      lst += ((gid, "M000", "T001", imei, 1.0))
      lst += ((gid, "M000", "T006", account, 1.0))
      lst += ((gid, "M000", "T005", androidId, 1.0))

      // 3-3、终端属性标签模块，包括：osName、manufacture、carrier、netType，权重为 1.0
      val osName = bean.osName // 操作系统名称
      val manufacture = bean.manufacture // 手机生产厂商（品牌）
      val carrier = bean.carrier // 用户所用的移动运营商
      val netType = bean.netType // 用户所用的网络类型（3g  4g  5g   wifi ）

      lst += ((gid, "M003", "T302", osName, 1.0))
      lst += ((gid, "M003", "T301", manufacture, 1.0))
      lst += ((gid, "M003", "T304", carrier, 1.0))
      lst += ((gid, "M003", "T303", netType, 1.0))

      // 3-4、活跃地域标签模块，包括：province、city、district，，权重为 1.0
      val province = bean.province
      val city = bean.city
      val district = bean.district

      lst += ((gid, "M014", "T141", province, 1.0))
      lst += ((gid, "M014", "T142", city, 1.0))
      lst += ((gid, "M014", "T143", district, 1.0))

      // 3-5、商品偏好标签模块 bean.event
      val event = bean.event // 本条日志所代表的事件的详细信息（所发生的页面，前一个页面，所点击的商品skuid...)

      // 3-6、取商品信息广播变量 goodsInfoDict
      val goodsInfoDict = bc.value

      // 3-7、标题 event->title ,需要 HanLP.segment 进行切分 ,过滤掉word大于1的值
      val titleOption = event.get("title")
      if (titleOption.isDefined) {
        import scala.collection.JavaConversions._
        val kwds = HanLP.segment(titleOption.get).map(_.word).filter(_.size > 1).map(w => (gid, "M009", "T094", w, 1.0))
        lst ++= kwds
      }

      // 3-8、所收藏的商品id  event->skuid
      val skuid = event.get("skuid")
      if (skuid.isDefined) {
        // 取商品信息广播变量中的 skuid
        val goodsInfoOption = goodsInfoDict.get(skuid.get)
        if (goodsInfoOption.isDefined) {
          // Array(pname,brand,c1,c2,c3)
          val goodsInfo: Array[String] = goodsInfoOption.get
          lst += ((gid, "M009", "T092", goodsInfo(1), 1.0))
          lst += ((gid, "M009", "T091", goodsInfo(2), 1.0))
          lst += ((gid, "M009", "T091", goodsInfo(3), 1.0))
          lst += ((gid, "M009", "T091", goodsInfo(4), 1.0))
        }
      }
      lst
    })
  }

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkUtil.getSparkSession(this.getClass.getSimpleName)
    val eventlog_path = "user_profile/data/output/eventlog/day01"
    val eventlog: RDD[(Long, String, String, String, Double)] = extractEventLogTags(spark, eventlog_path)
    eventlog.foreach(println)
  }
}
