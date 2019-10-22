package cn.doitedu.course.dw.dict

import java.util.Properties

import org.apache.spark.sql.SparkSession

object AreaDict2 {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName(this.getClass.getSimpleName)
      .master("local[*]")
      .getOrCreate()

    val props = new Properties()
    props.setProperty("user","root")
    props.setProperty("password","root")
    val df = spark.read.jdbc("jdbc:mysql://localhost:3306/demo","t_md_areas",props)

    df.createTempView("area")

    val df2 = spark.sql(
      """
        |
        |SELECT
        |	a.BD09_LNG,
        |	a.BD09_LAT,
        |	d.areaname AS province,
        |	c.areaname AS city,
        |	b.areaname AS district
        |FROM
        |  area a
        |JOIN area b ON a. LEVEL = 4  AND a.PARENTID = b.id
        |JOIN area c ON b.parentid = c.id
        |JOIN area d ON c.parentid = d.id
        |
      """.stripMargin)

    df2.coalesce(1).write.parquet("data_ware/data/dict/out_area_dict")

    spark.close()
  }
}
