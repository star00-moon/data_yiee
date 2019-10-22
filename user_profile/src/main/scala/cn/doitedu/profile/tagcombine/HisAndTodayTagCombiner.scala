package cn.doitedu.profile.tagcombine

import java.io.File
import java.util.Properties

import cn.doitedu.commons.utils.{FileUtils, SparkUtil}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Dataset, Row}


/**
  * @author: 余辉
  * @blog: https://blog.csdn.net/silentwolfyh
  * @create: 2019/10/19
  * @description:
  *   功能：将当日的标签计算结果，整合历史（前一日）标签结果
  *   要考虑的点：
  *   1. 当日可能有新的人
  *   2. 历史记录中的人，当日可能没出现
  *   3. 历史记录中的人，当日有新标签
  *   4. 历史记录中的人，当日出现出现过的标签
  *   5. 历史记录中的人，有标签当日没有出现
  *   总结出来就是：
  *   -- 对有权重的数据
  *   历史标签  full join  当日标签
  *   2. 历史有，当日有，权重累加
  *   3. 历史有，当日无，权重衰减  (应该有衰减系数字典)
  *   4. 历史无，当日有，取当日
  *   -- 对无权重的数据
  *   无权重标签，都是数仓报表中出来的，而数仓报表的数据，每天抽过来其实都是历史以来积累到当日的全量结果！
  *   直接取当日数据！
  **/
case class TagJoinBean(gid: java.lang.Long, tag_module: String, tag_name: String, tag_value: String, weight: java.lang.Double,
                       gid2: java.lang.Long, tag_module2: String, tag_name2: String, tag_value2: String, weight2: java.lang.Double)

object HisAndTodayTagCombiner {

  def main(args: Array[String]): Unit = {

    //1、日志级别设置 Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("org").setLevel(Level.WARN)

    // 2、建立session连接  import spark.implicits._
    val spark = SparkUtil.getSparkSession(this.getClass.getSimpleName)
    import spark.implicits._

    // 3、加载mysql中衰减系数字典
    val props = new Properties()
    props.setProperty("user", "root")
    props.setProperty("password", "123456")
    val decayMap = spark.read.jdbc("jdbc:mysql://localhost:3306/yieemeta", "tag_decay_dict", props)
      .selectExpr("concat(tag_module,tag_name) as key", "decay")
      .map({
        case Row(key: String, decay: Double) => (key, decay)
      }).rdd.collectAsMap()
    val bc = spark.sparkContext.broadcast(decayMap)

    // 4、加载day01和day02的标签
    val tags01 = spark.read.parquet("user_profile/data/output/tags/day01")
    val tags02 = spark.read.parquet("user_profile/data/output/tags/day02")

    // 5、过滤出有权重的数据和无权重的数据(可以在上游优化，上游就应该把有权重和无权重的数据分开存储)
    val haveWeight01 = tags01.where("weight!=-9999.9")
    val haveWeight02 = tags02.where("weight!=-9999.9")

    // 6、full  join
    val allWeight = haveWeight01.join(haveWeight02.toDF("gid2", "tag_module2", "tag_name2", "tag_value2", "weight2")
      , 'gid === 'gid2 and 'tag_module === 'tag_module2 and 'tag_name === 'tag_name2 and 'tag_value === 'tag_value2
      , "full_outer")

    allWeight.show(100, false)

    /**
      * +----+----------+--------+---------+------+----+-----------+---------+----------+-------+
      * |gid |tag_module|tag_name|tag_value|weight|gid2|tag_module2|tag_name2|tag_value2|weight2|
      * +----+----------+--------+---------+------+----+-----------+---------+----------+-------+
      * |1001|M012      |T121    |高钙低脂     |2.0   |1001|M012       |T121     |高钙低脂      |1.0    |
      * |1002|M012      |T121    |flink成神  |1.0   |1002|M012       |T121     |flink成神   |1.0    |
      * |1002|M012      |T121    |spark技术  |2.0   |1002|M012       |T121     |spark技术   |2.0    |
      * |1002|M012      |T121    |玄幻剧      |1.0   |null|null       |null     |null      |null   |
      * |1001|M000      |T001    |imei001  |2.0   |null|null       |null     |null      |null   |
      * |1002|M012      |T121    |刘亦菲      |1.0   |null|null       |null     |null      |null   |
      * |1002|M012      |T121    |hadoop精通 |1.0   |1002|M012       |T121     |hadoop精通  |1.0    |
      * |1001|M000      |T001    |imei011  |4.0   |null|null       |null     |null      |null   |
      * |null|null      |null    |null     |null  |1012|M000       |T001     |mac012    |2.0    |
      * |null|null      |null    |null     |null  |1001|M012       |T121     |风流倜傥      |1.0    |
      * |null|null      |null    |null     |null  |1001|M000       |T001     |idfa01    |2.0    |
      * |1003|M003      |T303    |东湖区      |2.0   |1003|M003       |T303     |东湖区       |2.0    |
      * |null|null      |null    |null     |null  |1001|M012       |T121     |碎花小裙      |2.0    |
      */

    val ds: Dataset[TagJoinBean] = allWeight.as[TagJoinBean]

    // 7、处理有权重标签
    val haveWeightResult = ds.rdd.map(bean => {

      val decayDict = bc.value
      val decay = decayDict.getOrElse(bean.tag_module + bean.tag_name, 1.0)

      var gid = bean.gid
      var tag_module = bean.tag_module
      var tag_name = bean.tag_name
      var tag_value = bean.tag_value
      var weight = bean.weight

      val w1 = bean.weight
      val w2 = bean.weight2

      if (w1 != null && w2 != null) {
        weight = w1 + w2
      }

      if (w1 != null && w2 == null) {
        weight = w1 * decay
      }

      if (w1 == null && w2 != null) {
        gid = bean.gid2
        tag_module = bean.tag_module2
        tag_name = bean.tag_name2
        tag_value = bean.tag_value2
        weight = w2
      }

      (gid, tag_module, tag_name, tag_value, weight)
    })
      .toDF("gid", "tag_module", "tag_name", "tag_value", "weight")

    /**
      * 逻辑是：   历史有，今天无，取历史
      */

    // 8、取当日的无权重标签结果
    val noWeight01 = tags01.where("weight=-9999.9")
    val noWeight02 = tags02.where("weight=-9999.9")

    noWeight01.createTempView("no1")
    noWeight02.createTempView("no2")
    val noWeightResult = spark.sql(
      """
        |
        |select
        |if(no2.gid is not null,no2.gid,no1.gid) as gid,
        |if(no2.tag_module is not null,no2.tag_module,no1.tag_module) as tag_module,
        |if(no2.tag_name is not null,no2.tag_name,no1.tag_name) as tag_name,
        |if(no2.tag_value is not null,no2.tag_value,no1.tag_value) as tag_value,
        |-9999.9 as weight
        |from no1 full join no2
        |on no1.gid=no2.gid and no1.tag_module=no2.tag_module and no1.tag_name = no2.tag_name and no1.tag_value = no2.tag_value
        |
      """.stripMargin)

    // 9、整合最终结果（有权重 + 无权重）
    val result = haveWeightResult.union(noWeightResult)

    // 10、保存数据
    result.show(100, false)
    FileUtils.deleteDir(new File("user_profile/data/output/tag_merge/day02"))
    result.coalesce(1).write.parquet("user_profile/data/output/tag_merge/day02")

    // 11、spark关闭
    spark.close()
  }
}
