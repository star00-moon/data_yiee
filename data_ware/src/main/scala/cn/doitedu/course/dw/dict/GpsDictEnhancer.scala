package cn.doitedu.course.dw.dict

import ch.hsr.geohash.GeoHash
import cn.doitedu.commons.utils.SparkUtil
import com.alibaba.fastjson.JSON
import org.apache.commons.io.IOUtils
import org.apache.commons.lang3.StringUtils
import org.apache.http.client.methods.{CloseableHttpResponse, HttpGet}
import org.apache.http.impl.client.HttpClientBuilder
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode}

/**
  * @date: 2019/8/27
  * @site: www.doitedu.cn
  * @author: hunter.d 涛哥
  * @qq: 657270652
  * @description: 技能：在spark中请求web api
  *               将流量日志预处理时，
  *               解析不出省市区的gps坐标，
  *               去高德地图开放api上解析一波，
  *               并将结果加强到我们公司自己的字典中
  */
object GpsDictEnhancer {

  def main(args: Array[String]): Unit = {
    val spark = SparkUtil.getSparkSession()
    import spark.implicits._

    val toparse: DataFrame = spark.read.parquet("data_ware/data/toparse_gps/2019-06-16")

    // mappartitions的本质就是，task对一个分区调用一次你给的函数
    val res: RDD[(String, String, String, String)] = toparse.rdd.mapPartitions(iter=>{
      // 先构造http客户端
      val client = HttpClientBuilder.create().build()

      // 然后再去逐行处理这个分区中的每条数据
      val x: Iterator[(String, String, String, String)] = iter.map(row=>{

        val lng = row.getDouble(0)
        val lat = row.getDouble(1)

        // 构造一个request请求对象
        val get = new HttpGet(s"https://restapi.amap.com/v3/geocode/regeo?key=d5717203b89f0635dab00bd1594c419f&location=${lng},${lat}")


        val response: CloseableHttpResponse = client.execute(get)

        val in = response.getEntity.getContent

        val lst = IOUtils.readLines(in)
        import scala.collection.JavaConversions._
        val json = lst.mkString("")


        // println(json)
        val jsonobj = JSON.parseObject(json)
        var p = ""
        var c = ""
        var d = ""

        val status = jsonobj.get("status").toString
        if (status.equals("1")) {
          val component = jsonobj.getJSONObject("regeocode").getJSONObject("addressComponent")
          p = component.getString("province")
          c = component.getString("city")
          d = component.getString("district")
        }

        val geo = GeoHash.withCharacterPrecision(lat, lng, 5).toBase32
        (geo, p, c, d)


      })

      client.close()
      x
    })

    res
      .filter(tp => StringUtils.isNotBlank(tp._2))
      .toDF("geo", "province", "city", "district")
      // spark.sql.shuffle.partitions=200
      .coalesce(1)
      .write
      .mode(SaveMode.Append)
      .parquet("data_ware/data/dict/out_area_dict")


    spark.close()

  }
}
