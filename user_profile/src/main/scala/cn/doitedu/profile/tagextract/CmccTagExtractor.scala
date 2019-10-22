package cn.doitedu.profile.tagextract

import cn.doitedu.commons.utils.SparkUtil
import com.hankcs.hanlp.HanLP
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ListBuffer

/**
  * @author: 余辉 https://blog.csdn.net/silentwolfyh
  * @create: 2019/10/18
  * @description:
  * 1、CMCC流量数据的标签抽取
  * 2、返回：gid，模块，便签，值，权重 (Long, String, String, String, Double)
  **/
object CmccTagExtractor {

  def extractCmccTags(spark: SparkSession, path: String): RDD[(Long, String, String, String, Double)] = {

    // 1、文件的schema字段为： "gid", "phone", "imei", "imsi", "manufacture", "title", "category"
    val df = spark.read.parquet(path)
    df.printSchema()

    // 2、数据映射成标签数据，gid，模块，便签，值，权重
    df.rdd.flatMap(row => {

      // 2-1 建立一个ListBuffer 存储数据
      val lst = new ListBuffer[(Long, String, String, String, Double)]

      // 2-2、全局统一标识id gid
      val gid = row.getAs[Long]("gid")

      // 2-3、标识标签模块,包括：phone、imei、imsi、manufacture 权重为 1.0
      val phone = row.getAs[String]("phone")
      val imei = row.getAs[String]("imei")
      val imsi = row.getAs[String]("imsi")
      val manufacture = row.getAs[String]("manufacture")

      lst += ((gid, "M000", "T008", phone, 1))
      lst += ((gid, "M000", "T001", imei, 1))
      lst += ((gid, "M000", "T002", imsi, 1))
      lst += ((gid, "M003", "T301", manufacture, 1))

      // 2-3、标题 itle ,需要 HanLP.segment 进行切分 ,过滤掉word大于1的值
      val title = row.getAs[String]("title")
      import scala.collection.JavaConversions._
      val kwds = HanLP.segment(title).map(_.word).filter(_.size > 1).map(w => (gid, "M013", "T131", w, 1.0))
      lst ++= kwds

      // 2-4、商品偏好属性 category ，切分方式 【\\|】
      val category = row.getAs[String]("category")
      val cats = category.split("\\|").map(cat => (gid, "M009", "T091", cat, 1.0))
      lst ++= cats
    })
  }

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkUtil.getSparkSession(this.getClass.getSimpleName)
    val cmcclog_path = "user_profile/data/output/cmcc/day01"
    val cmcclogRdd: RDD[(Long, String, String, String, Double)] = extractCmccTags(spark, cmcclog_path)
    println("cmcclogRdd.count()==>", cmcclogRdd.count())
    cmcclogRdd.take(10).foreach(println)
  }
}
