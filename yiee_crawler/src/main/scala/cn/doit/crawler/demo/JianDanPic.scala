package cn.doit.crawler.demo

import java.io.FileOutputStream

import org.apache.commons.io.{FileUtils, IOUtils}
import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.HttpClientBuilder
import org.jsoup.Jsoup

/**
 * @author: 余辉
 * @blog:   https://blog.csdn.net/silentwolfyh
 * @create: 2019/10/22
 * @description: 煎蛋网妹子图爬取
 **/
object JianDanPic {

  def main(args: Array[String]): Unit = {

    val client = HttpClientBuilder.create().build()

    for (i <- 1 to 14) {
      println(s"正在爬取第 ${i} 页......................................")
      val page = s"http://jandan.net/ooxx/page-${i}#comments"
      val doc = Jsoup.connect(page).get()

      val elements = doc.getElementsByClass("view_img_link")
      import scala.collection.JavaConversions._


      for (atag <- elements) {
        val href = atag.attr("href")
        val picName = href.substring(href.lastIndexOf("/") + 1)
        println(s"正在爬取 ${picName} 图片----------")
        val get = new HttpGet("http:" + href)
        val in = client.execute(get).getEntity.getContent
        val out = new FileOutputStream(s"C:/Work/pics/${picName}")
        IOUtils.copy(in, out)

        IOUtils.closeQuietly(in)
        IOUtils.closeQuietly(out)
        Thread.sleep(1000)
      }
    }

    client.close()


  }

}
