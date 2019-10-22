//package cn.doit.crawler.demo
//
//import java.io.FileOutputStream
//import java.util
//
//import org.apache.commons.io.IOUtils
//import org.apache.http.Header
//import org.apache.http.client.methods.HttpGet
//import org.apache.http.impl.client.HttpClientBuilder
//import org.apache.http.message.BasicHeader
//import org.jsoup.Jsoup
//import org.jsoup.nodes.Document
//import org.jsoup.select.Elements
//
///**
//  * @date: 2019/9/16
//  * @site: www.doitedu.cn
//  * @author: hunter.d 涛哥
//  * @qq: 657270652
//  * @description: Jsoup demo示例程序
//  */
//object JsoupDemo {
//
//  def main(args: Array[String]): Unit = {
//
//    //    val doc = Jsoup.connect("http://www.appchina.com/category/30/4_1_1_3_0_0_0.html").get()
//    //    val dtl = Jsoup.connect("http://www.appchina.com/app/com.xiaoji.quickbass.merchant").get()
//    /*val index = Jsoup.connect("http://www.appchina.com/").get()
//    println(index)*/
//
//    val header1 = new BasicHeader("User-Agent","Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/65.0.3314.0 Safari/537.36 SE 2.X MetaSr 1.0")
//    val header2 = new BasicHeader("Referer","http://www.appchina.com/app/com.xiaoji.quickbass.merchant")
//
//    val headers = new util.ArrayList[Header]()
//    headers.add(header1)
//    headers.add(header2)
//
//
//    val client = HttpClientBuilder.create()
//      .setDefaultHeaders(headers)
//      .build()
//    val get = new HttpGet("http://www.appchina.com/")
//    val content = client.execute(get).getEntity.getContent
//    val lst = IOUtils.readLines(content)
//    import scala.collection.JavaConversions._
//    for(line<-lst){
//      println(line)
//    }
//
//
//    client.close()
//
//    //println(doc)
//    //    println(dtl)
//
//
//  }
//
//}
