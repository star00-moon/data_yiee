package demo.graphx

import java.io.File

import cn.doitedu.commons.utils.{FileUtils, SparkUtil}
import org.apache.commons.lang3.StringUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{DataTypes, StructType}

/**
  * @author: 余辉
  * @blog: https://blog.csdn.net/silentwolfyh
  * @create: 2019/10/22
  * @description: 利用idmapping（id映射字典）来对日志进行加工，为每条原始日志添加一个gid字段
  *               1、得到idmapping（id映射字典）==》 idmapping
  *               2、获取原始数据==》logDf
  *               3、idmapping 关联 logDf 得到新数据
  **/
object Demo1_2 {
  def main(args: Array[String]): Unit = {
    //1、建立Session import spark.implicits._
    val spark: SparkSession = SparkUtil.getSparkSession(this.getClass.getTypeName)
    import spark.implicits._

    // 2、加载idmapping字典【gid为Long类型，id为String类型】, 通过  (id, gid) 转为Map集合
    val idmapping: collection.Map[String, Long] =
      spark
        .read
        .parquet("user_profile/demodata/graphx/out_idmp")
        .rdd.map(row => {
        val gid: Long = row.getAs[Long]("gid")
        val id: String = row.getAs[String]("id")
        (id, gid)
      }).collectAsMap()

    // 3、idmapping 加载到广播变量
    val bc: Broadcast[collection.Map[String, Long]] = spark.sparkContext.broadcast(idmapping)

    //4、创建原始数据的schema,字段类型为：DataTypes.StringType  phone,name,wx,income
    val schema: StructType = new StructType()
      .add("phone", DataTypes.StringType)
      .add("name", DataTypes.StringType)
      .add("wx", DataTypes.StringType)
      .add("income", DataTypes.IntegerType)

    // 5、加载日志数据，使用 schema + 原始日志，转为 logDf
    val logDf: DataFrame = spark.read.schema(schema).option("header", true).csv("user_profile/demodata/graphx/input/demo1.dat")
    logDf.printSchema()

    //6、日志转为DataFrame, idmapping 关联 logDf 得到新数据
    val gidLogDF: DataFrame = logDf.rdd.map(row => {
      // 6-1、从广播变量中取出id映射字典
      val idmp: collection.Map[String, Long] = bc.value

      // 6-2、获取原始日志中：phone、name、wx、income
      val phone: String = row.getAs[String]("phone")
      val name: String = row.getAs[String]("name")
      val wx: String = row.getAs[String]("wx")
      val income: Int = row.getAs[Int]("income")

      // 6-3、判空，获取数组的第一个值，作为组id
      val notNullId: String = Array(phone, name, wx).filter(StringUtils.isNoneBlank(_))(0)

      // 6-4、通过组id，在广播变量中获取gid
      val gidOption: Option[Long] = idmp.get(notNullId)

      // 6-5、如果没有值则取  "未知"，有值则转为字符串
      var gid: String = "未知"
      if (gidOption.isDefined) gid = gidOption.get + ""

      // 6-6、通过Tuple返回  (gid, phone, name, wx, income)
      (gid, phone, name, wx, income)
    }).toDF("gid", "phone", "name", "wx", "income")

    //7、数据存储
    FileUtils.deleteDir(new File("user_profile/demodata/graphx/out_gidlog"))
    gidLogDF.coalesce(1)
      .write
      .option("header", true)
      .csv("user_profile/demodata/graphx/out_gidlog")

    /** *
      * +-----------+-----------+----+------+------+
      * |gid        |phone      |name|wx    |income|
      * +-----------+-----------+----+------+------+
      * |-1485777898|13866778899|刘德华 |wx_hz |2000  |
      * |-1485777898|13877669988|华仔  |wx_hz |3000  |
      * |-1485777898|null       |刘德华 |wx_ldh|5000  |
      * |-1095633001|13912344321|马德华 |wx_mdh|12000 |
      * |-1095633001|13912344321|二师兄 |wx_bj |3500  |
      * |-1095633001|13912664321|猪八戒 |wx_bj |5600  |
      * +-----------+-----------+----+------+------+
      */

    //8、spark关闭
    spark.close()
  }
}
