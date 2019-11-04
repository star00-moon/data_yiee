package cn.doit.crawler.demo

import java.io.{FileOutputStream, InputStream}

import org.apache.commons.io.{FileUtils, IOUtils}
import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.{CloseableHttpClient, HttpClientBuilder}
import org.jsoup.Jsoup
import org.jsoup.nodes.Document
import org.jsoup.select.Elements

/**
  * @author: 余辉
  * @blog: https://blog.csdn.net/silentwolfyh
  * @create: 2019/10/22
  * @description: 煎蛋网妹子图爬取
  **/
object JianDanPic {

  def main(args: Array[String]): Unit = {

    // 1、创建客户端
    val client: CloseableHttpClient = HttpClientBuilder.create().build()

    // 2、循序1到14页
    for (i <- 1 to 14) {

      println(s"正在爬取第 ${i} 页......................................")
      //  2-1、获取url，发送请求
      val page = s"http://jandan.net/ooxx/page-${i}#comments"
      val doc: Document = Jsoup.connect(page).get()

      // 2-2、获取子节点 class 为：view_img_link
      val elements: Elements = doc.getElementsByClass("view_img_link")
      import scala.collection.JavaConversions._

      // 2-3、获取 elements 中每一个 a 便签 ，且保存
      for (atag <- elements) {
        val href: String = atag.attr("href")
        val picName: String = href.substring(href.lastIndexOf("/") + 1)
        println(s"正在爬取 ${picName} 图片----------")
        val get = new HttpGet("http:" + href)
        val in: InputStream = client.execute(get).getEntity.getContent
        val out = new FileOutputStream(s"yiee_crawler/data/${picName}")
        IOUtils.copy(in, out)
        IOUtils.closeQuietly(in)
        IOUtils.closeQuietly(out)
        Thread.sleep(1000)
      }
    }
    // 3、客户端关闭
    client.close()
  }
}
