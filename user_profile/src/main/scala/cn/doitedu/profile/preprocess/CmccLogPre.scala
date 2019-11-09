package cn.doitedu.profile.preprocess

import cn.doitedu.commons.utils.{DictsLoader, SparkUtil}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}


/**
 * @author: 余辉
 * @blog:   https://blog.csdn.net/silentwolfyh
 * @create: 2019/10/22
 * @description: cmcc第三方数据预处理
 **/
object CmccLogPre {

  def main(args: Array[String]): Unit = {
    // 1、建立session连接  import spark.implicits._
    val spark: SparkSession = SparkUtil.getSparkSession(this.getClass.getSimpleName)
    import spark.implicits._

    // 2、加载加载idmapping映射字典放入广播地址
    val idmp: collection.Map[Long, Long] = DictsLoader.loadIdmpDict(spark, "user_profile/data/output/idmp/day01")
    val bcIdmp: Broadcast[collection.Map[Long, Long]] = spark.sparkContext.broadcast(idmp)

    // 3、加载加载url内容信息字典放入广播地址
    val urlContent: collection.Map[String, (String, String)] = DictsLoader.loadUrlContentDict(spark, "yiee_crawler/data/jdgoods")
    val bcUrl: Broadcast[collection.Map[String, (String, String)]] = spark.sparkContext.broadcast(urlContent)

    // 4、加载cmcc原始日志
    val ds: Dataset[String] = spark.read.textFile("user_profile/data/cmcclog/day01")

    // 4-1、rdd map 字段切分 \t
    val res: DataFrame = ds
      .map(line => {
        line.split("\t", -1)
      })
      .filter(arr => {
        // 4-2、过滤条件一： 字段大于40
        val flag1: Boolean = arr.length > 40
        val bool: Boolean = arr.length > 40
        // 4-3、过滤条件二： 数组6,7,8不为空，左右空格去掉判断空值
        val flag2: Boolean = !Array(arr(6).trim, arr(7).trim, arr(8).trim).mkString("").equals("")
        flag1 && flag2
      }
      )
      // 4-4、使用 mapPartitions 处理
      .mapPartitions(iter => {
        try {
          // 4-4-1、广播变量提取 bcIdmp 和 bcUrl
          val idmp: collection.Map[Long, Long] = bcIdmp.value
          val urlContent: collection.Map[String, (String, String)] = bcUrl.value

          // 4-4-2、获取cmcc中的 (gid, phone, imei, imsi, manufacture, title, category)
          iter.map(arr => {
            // 4-4-2-1、提取 phone=》arr(6)、imei=>arr(7)、 imsi=》arr(8)、 url=》arr(28)、 manufacture=》arr(30)
            val phone: String = arr(6)
            val imei: String = arr(7)
            val imsi: String = arr(8)
            val url: String = arr(28)
            val manufacture: String = arr(30)

            // 4-4-2-2、集成字典信息 idmp,根据 (phone, imei, imsi) 取不为空的phone
            val id: String = Array(phone, imei, imsi).filter(StringUtils.isNoneBlank(_)).head
            val gid: Long = idmp.getOrElse(id.hashCode.toLong, -1L)

            // 4-4-2-3、url内容知识库、app描述信息
            val content: (String, String) = urlContent.getOrElse(url, ("", ""))
            val title: String = content._1
            val category: String = content._2

            // 4-4-2-4、返回 (gid, phone, imei, imsi, manufacture, title, category)
            (gid, phone, imei, imsi, manufacture, title, category)
          })
        } catch {
          // 4-4-3、异常处理 case e:List.empty ， case _: List.empty
          case e: Exception => List.empty[(Long, String, String, String, String, String, String)].toIterator
          case _: Throwable => List.empty[(Long, String, String, String, String, String, String)].toIterator
        }
      })
      // 4-5、过滤gid为-1
      .filter(_._1 != -1)
      // 4-6、转为df  "gid", "phone", "imei", "imsi", "manufacture", "title", "category"
      .toDF("gid", "phone", "imei", "imsi", "manufacture", "title", "category")

    // 5、输出保存结果 ，且 spark 关闭
    res.write.parquet("user_profile/data/output/cmcc/day01")
    spark.close()
  }
}

/** *
  *
  * +-----------+-------------+---------------+----------------+---------------------------------------------------------------------------------------------------------------------------------------------------+-----+--------+
  * |gid        |phone        |imei           |imsi            |manufacture                                                                                                                                        |title|category|
  * +-----------+-------------+---------------+----------------+---------------------------------------------------------------------------------------------------------------------------------------------------+-----+--------+
  * |-1418642797|8618337131286|460025371477339|8617530063470200|Dalvik/1.6.0 (Linux; U; Android 4.0.3; X907 Build/IML74K)                                                                                          |     |        |
  * |-1418642797|8618337131286|460025371477339|8617530063470200|Dalvik/1.6.0 (Linux; U; Android 4.0.3; X907 Build/IML74K)                                                                                          |     |        |
  * |-1418642797|8618337131286|460025371477339|8617530063470200|Dalvik/1.6.0 (Linux; U; Android 4.0.3; X907 Build/IML74K)                                                                                          |     |        |
  * |-1418642797|8618337131286|460025371477339|8617530063470200|Dalvik/1.6.0 (Linux; U; Android 4.0.3; X907 Build/IML74K)                                                                                          |     |        |
  * |-1418642797|8618337131286|460025371477339|8617530063470200|Dalvik/1.6.0 (Linux; U; Android 4.0.3; X907 Build/IML74K)                                                                                          |     |        |
  * |-1418642797|8618337131286|460025371477339|8617530063470200|Dalvik/1.6.0 (Linux; U; Android 4.0.3; X907 Build/IML74K)                                                                                          |     |        |
  * |-1418642797|8618337131286|460025371477339|8617530063470200|Dalvik/1.6.0 (Linux; U; Android 4.0.3; X907 Build/IML74K)                                                                                          |     |        |
  * |-1418642797|8618337131286|460025371477339|8617530063470200|Dalvik/1.6.0 (Linux; U; Android 4.0.3; X907 Build/IML74K)                                                                                          |     |        |
  * |-1418642797|8618337131286|460025371477339|8617530063470200|Dalvik/1.6.0 (Linux; U; Android 4.0.3; X907 Build/IML74K)                                                                                          |     |        |
  *
  */
