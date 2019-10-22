package cn.doitedu.profile.preprocess

import cn.doitedu.commons.utils.{DictsLoader, SparkUtil}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.DataFrame


/**
 * @author: 余辉
 * @blog:   https://blog.csdn.net/silentwolfyh
 * @create: 2019/10/22
 * @description: cmcc第三方数据预处理
 **/
object CmccLogPre {

  def main(args: Array[String]): Unit = {
    // 1、建立session连接  import spark.implicits._
    val spark = SparkUtil.getSparkSession(this.getClass.getSimpleName)
    import spark.implicits._

    // 2、加载加载idmapping映射字典放入广播地址
    val idmp: collection.Map[Long, Long] = DictsLoader.loadIdmpDict(spark, "user_profile/data/output/idmp/day01")
    val bcIdmp = spark.sparkContext.broadcast(idmp)

    // 3、加载加载url内容信息字典放入广播地址
    val urlContent = DictsLoader.loadUrlContentDict(spark, "yiee_crawler/data/jdgoods")
    val bcUrl = spark.sparkContext.broadcast(urlContent)

    // 4、加载cmcc原始日志
    val ds = spark.read.textFile("user_profile/data/cmcclog/day01")
    val res = ds
      .map(line => {
        // 4-1、抽取字段
        line.split("\t", -1)
      })
      .filter(arr => {
        val flag1 = arr.size > 40
        val flag2 = !Array(arr(6).trim, arr(7).trim, arr(8).trim).mkString("").equals("")
        flag1 && flag2
      }
      )
      .mapPartitions(iter => {
        try {
          val idmp = bcIdmp.value
          val urlContent: collection.Map[String, (String, String)] = bcUrl.value
          iter.map(arr => {
            val phone = arr(6)
            val imei = arr(7)
            val imsi = arr(8)
            val url = arr(28)
            val manufacture = arr(30)
            // 4-2、集成字典信息（idmp、url内容知识库、app描述信息）
            val id = Array(phone, imei, imsi).filter(StringUtils.isNoneBlank(_)).head
            val gid = idmp.getOrElse(id.hashCode.toLong, -1L)

            val content = urlContent.getOrElse(url, ("", ""))
            val title = content._1
            val category = content._2
            (gid, phone, imei, imsi, manufacture, title, category)
          })
        } catch {
          case e: Exception => List.empty[(Long, String, String, String, String, String, String)].toIterator
          case _: Throwable => List.empty[(Long, String, String, String, String, String, String)].toIterator
        }
      })
      .filter(_._1 != -1)
      .toDF("gid", "phone", "imei", "imsi", "manufacture", "title", "category")

    res.show(10, false)
    // 5、输出保存结果
    res.write.parquet("user_profile/data/output/cmcc/day01")

    spark.close()

  }

}
