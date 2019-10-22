package demo.graphx

import java.io.File

import cn.doitedu.commons.utils.{FileUtils, SparkUtil}
import org.apache.commons.lang3.StringUtils
import org.apache.log4j.{Level, Logger}
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
    Logger.getLogger("org").setLevel(Level.WARN)
    val spark = SparkUtil.getSparkSession(this.getClass.getSimpleName)
    import spark.implicits._

    // 2、加载idmapping字典 "user_profile/demodata/graphx/out_idmp"，字段为 id，gid ,变成tpule2
    val idmapping = spark
      .read
      .parquet("user_profile/demodata/graphx/out_idmp")
      .rdd
      .map(row => {
        val gid = row.getAs[Long]("gid")
        val id = row.getAs[String]("id")
        (id, gid)
      }).collectAsMap()

    // 3、idmapping 加载到广播变量
    val bc = spark.sparkContext.broadcast(idmapping)

    //4、创建原始数据的schema
    val schema = new StructType()
      .add("phone", DataTypes.StringType)
      .add("name", DataTypes.StringType)
      .add("wx", DataTypes.StringType)
      .add("income", DataTypes.IntegerType)

    // 5、加载日志数据 schema + 原始日志
    val logDf = spark.read.schema(schema).option("header", true).csv("user_profile/demodata/graphx/input/demo1.dat")
    logDf.printSchema()

    //6、日志转为DataFrame, idmapping 关联 logDf 得到新数据
    val gidLogDF = logDf.rdd.map(row => {
      // 6-1、从广播变量中取出id映射字典
      val idmp: collection.Map[String, Long] = bc.value

      val phone = row.getAs[String]("phone")
      val name = row.getAs[String]("name")
      val wx = row.getAs[String]("wx")
      val income = row.getAs[Int]("income")

      val notNullId = Array(phone, name, wx).filter(StringUtils.isNotBlank(_))(0)
      val gidOption = idmp.get(notNullId)
      var gid: String = "未知"
      if (gidOption.isDefined) gid = gidOption.get + ""

      (gid, phone, name, wx, income)
    }).toDF("gid", "phone", "name", "wx", "income")

    //7、数据存储
    FileUtils.deleteDir(new File("user_profile/demodata/graphx/out_gidlog"))
    gidLogDF.coalesce(1)
      .write
      .option("header", true)
      .csv("user_profile/demodata/graphx/out_gidlog")

    //8、spark关闭
    spark.close()
  }
}
