package cn.doitedu.course.demo

import com.alibaba.fastjson.JSON
import org.apache.commons.io.IOUtils
import org.apache.http.client.methods.{CloseableHttpResponse, HttpGet}
import org.apache.http.impl.client.HttpClientBuilder

object GaoDeApiDemo {

  def main(args: Array[String]): Unit = {

    // http客户端
    val client = HttpClientBuilder.create().build()

    // 构造一个request请求对象
    val get = new HttpGet("https://restapi.amap.com/v3/geocode/regeo?key=d5717203b89f0635dab00bd1594c419f&location=104.98142658396348,25.79007116128849")


    val response: CloseableHttpResponse = client.execute(get)

    val in = response.getEntity.getContent

    val lst = IOUtils.readLines(in)
    import scala.collection.JavaConversions._
    val json = lst.mkString("")

    // println(json)

    val jsonobj = JSON.parseObject(json)
    val component = jsonobj.getJSONObject("regeocode").getJSONObject("addressComponent")
    val p = component.getString("province")
    val c = component.getString("city")
    val d = component.getString("district")

    println(p,c,d)

    client.close()


  }

}
