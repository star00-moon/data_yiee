package cn.doitedu.course.dw.dict

import java.util.Properties

import ch.hsr.geohash.GeoHash
import org.apache.spark.sql.SparkSession

/**
 * @author: 余辉
 * @blog:   https://blog.csdn.net/silentwolfyh
 * @create: 2019/10/22
 * @description: 地理位置名称字典构建程序
 **/
object AreaDict {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName(this.getClass.getSimpleName)
      .master("local[*]")
      .getOrCreate()

    val props = new Properties()
    props.setProperty("user", "root")
    props.setProperty("password", "root")

    // 加载映射mysql中的地理位置字典表
    /**
      * CREATE TABLE `t_area_dict` (
      * `BD09_LNG` double DEFAULT NULL,
      * `BD09_LAT` double DEFAULT NULL,
      * `province` text NOT NULL,
      * `city` text NOT NULL,
      * `district` text NOT NULL
      * ) ENGINE=InnoDB DEFAULT CHARSET=utf8;
      */
    val df = spark.read.jdbc("jdbc:mysql://localhost:3306/demo", "t_area_dict", props)


    /**
      * 将dataframe按rdd来做（拆解row）
      */
    import spark.implicits._
    val geoDF = df.map(row => {
      val lng = row.getAs[Double]("BD09_LNG")
      val lat = row.getAs[Double]("BD09_LAT")
      val province = row.getAs[String]("province")
      val city = row.getAs[String]("city")
      val district = row.getAs[String]("district")

      val geo = GeoHash.withCharacterPrecision(lat, lng, 5).toBase32

      (geo, province, city, district)
    })
      .distinct()
      .toDF("geo", "province", "city", "district")


    //df.show(10,false)
    geoDF.coalesce(1).write.parquet("data_ware/data/dict/out_area_dict")


    /**
      * 按结构化数据处理，用自定义sql函数，用临时表查询实现
      */

    // 自定义一个经纬度转geohash编码的自定义函数
    import org.apache.spark.sql.functions._

    val gps2geo = (lng:Double,lat:Double)=>{GeoHash.withCharacterPrecision(lat, lng, 5).toBase32}

    // 将一个scala的function注册到sql引擎中，成为sql引擎中的一个“sql函数”
    spark.udf.register("gps2geo",gps2geo)
    df.createTempView("area")

    spark.sql(
      """
        |
        |select
        |gps2geo(BD09_LNG,BD09_LAT) as geo,
        |province,
        |city,
        |district
        |
        |from area
        |
      """.stripMargin)



    /**
      * 按结构化数据处理，用自定义函数，用DSL风格查询实现
      */
    // 将一个普通的scala函数，装入一个sparksql的udf函数中，它就变成了一个sql的udf函数了
    // 不用注册，就可以在DSL风格的调用中使用了
    val gps2geo_2 = udf(gps2geo)
    val res = df.select(gps2geo_2('BD09_LNG,'BD09_LAT).as("geo"),'province,'city,'district)

    res.show(10,false)



    spark.close()

  }

}
