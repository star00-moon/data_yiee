package cn.doitedu.course.dw.dict

import cn.doitedu.commons.utils.SparkUtil

/**
 * @author: 余辉
 * @blog:   https://blog.csdn.net/silentwolfyh
 * @create: 2019/10/22
 * @description: 地理位置字典构建程序、方案3实现代码
 **/

case class Bean(id:String,areaName:String,pid:String,level:String,longitude:String,latitude:String)


object AreaDict3 {

  def main(args: Array[String]): Unit = {

    val spark = SparkUtil.getSparkSession()
    import spark.implicits._

    val df = spark.read.text("data_ware/data/dict/rawdata")


    val area = df.map(row=>{
      val line = row.getAs[String]("value")
      val arr = line.split("")

      val id = arr(4).substring(0,arr(4).length-1)
      val areaName = arr(5).substring(0,arr(5).length-1)
      val pid = arr(7).substring(0,arr(5).length-1)
      val level = arr(8).substring(0,arr(5).length-1)
      val longitude = arr(13).substring(0,arr(5).length-1)
      val latitude = arr(14).substring(0,arr(5).length-2)

      // 把数据结构化：用tuple组装，或者用case class组装即可
      //Bean(id,areaName,pid,level,longitude,latitude)
      (id,areaName,pid,level,longitude,latitude)
    }).toDF("id","name","pid","level","lng","lat")


    area.createTempView("area")

    val areaDict = spark.sql(
      """
        |
        |SELECT
        |	a.lng,
        |	a.lat,
        |	d.name AS province,
        |	c.name AS city,
        |	b.name AS district
        |FROM
        |  area a
        |JOIN area b ON a. LEVEL = '4'  AND a.pid = b.id
        |JOIN area c ON b.pid = c.id
        |JOIN area d ON c.pid = d.id
        |
      """.stripMargin)
    areaDict.coalesce(1).write.parquet("")

    spark.close()
  }
}
