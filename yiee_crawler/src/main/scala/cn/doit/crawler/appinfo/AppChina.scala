package cn.doit.crawler.appinfo

import java.io.{BufferedReader, BufferedWriter, FileReader, FileWriter}

import org.jsoup.Jsoup

import scala.io.Source

/**
  * @date: 2019/9/18
  * @site: www.doitedu.cn
  * @author: hunter.d 涛哥
  * @qq: 657270652
  * @description: appchina应用下载市场app信息爬取程序
  *
  *               appchina也有一定的反爬措施：
  *               1. 监测客户端的useragent
  *               2. 监测同一个ip来的访问频率
  *               应对措施：设置useragent；
  *               不断更换代理服务器；
  *               适当降低请求频率；
  */
object AppChina {

  def main(args: Array[String]): Unit = {

//    getAppDtlUrls("yiee_crawler/data/appchina-dtlurls/dtl.urls")
    getAppDesc("yiee_crawler/data/appchina-dtlurls/dtl.urls", "yiee_crawler/data/appdesc/app.desc")

  }

  /***
    * 获取网页中的  appid appname appDtlUrl 保存到yiee_crawler/data/appchina-dtlurls/dtl.urls
    * @param urlSavePath
    */
  def getAppDtlUrls(urlSavePath: String) = {
    // 建立头文件内容
    val userAgent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.157 Safari/537.36"
    val cookie = "Hm_lvt_c1a192e4e336c4efe10f26822482a1a2=1568770426; Hm_lpvt_c1a192e4e336c4efe10f26822482a1a2=1568770426; UM_distinctid=16d420103a3367-05aee9bc53c7c5-353166-1fa400-16d420103a4371; CNZZDATA1257961818=393573309-1568768131-null%7C1568768131"
    val headers = Map("userAgent" -> userAgent, "referal" -> "http://www.baidu.com", "Cookie" -> cookie)

    //建立IO流，写入数据
    val bw = new BufferedWriter(new FileWriter(urlSavePath, true))

    // 循环翻页
    for (i <- 24 to 34) {
      val url = s"http://www.appchina.com/category/30/${i}_1_1_3_0_0_0.html"
      println(s"-----------${i}-----------")

      import scala.collection.JavaConversions._

      //程序执行且获取网页内容
      val doc = Jsoup.connect(url).headers(headers).execute().parse()

      //获取app-name的标签
      val h1tags = doc.getElementsByClass("app-name")

      //获取基础数据
      for (h1 <- h1tags) {
        // app名称
        val appname = h1.attr("title")

        //获取第一个链接
        val atag = h1.getElementsByTag("a").get(0)

        // app详情页地址
        val appDtlUrl = atag.attr("href")

        //app的id
        val appid = appDtlUrl.substring(appDtlUrl.lastIndexOf("/") + 1)

        //保存数据
        println(s"${appid}\001${appname}\001${appDtlUrl}")
        bw.write(s"${appid}\001${appname}\001${appDtlUrl}")
        bw.newLine()

        Thread.sleep(500)
      }
    }

    bw.flush()
    bw.close()
  }


  /***
    * 根据获取app清单，分别获取每个详细的app应用数据，保存数据为：app的id ， app名称 ， app相信信息
    * @param dtlUrls
    * @param resultSavePath
    */
  def getAppDesc(dtlUrls: String, resultSavePath: String): Unit = {
    // 建立头文件内容
    val userAgent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.157 Safari/537.36"
    val cookie = "Hm_lvt_c1a192e4e336c4efe10f26822482a1a2=1568770426; Hm_lpvt_c1a192e4e336c4efe10f26822482a1a2=1568770426; UM_distinctid=16d420103a3367-05aee9bc53c7c5-353166-1fa400-16d420103a4371; CNZZDATA1257961818=393573309-1568768131-null%7C1568768131"
    val headers = Map("userAgent" -> userAgent, "referal" -> "http://www.baidu.com", "Cookie" -> cookie)

    //建立IO流，写入数据
    val bw = new BufferedWriter(new FileWriter(resultSavePath, true))

    //  加载详情页地址列表
    val br = new BufferedReader(new FileReader("yiee_crawler/data/appchina-dtlurls/dtl.urls"))
    var line = br.readLine()

    // 循环遍历每一个地址，抓取详情页，提取app描述信息
    while (line != null) {

      val arr = line.split("\001")
      val url = arr(2)
      import scala.collection.JavaConversions._
      val doc = Jsoup.connect(s"http://www.appchina.com${url}").headers(headers).execute().parse()  //http://www.appchina.com/app/com.vpgame.eric

      // 提取app描述信息
      val p = doc.getElementsByClass("art-content").get(0)
      val appDesc = p.text()

      // app的id ， app名称 ， app相信信息
      println(s"${arr(0)}\001${arr(1)}\001${appDesc}")
      bw.write(s"${arr(0)}\001${arr(1)}\001${appDesc}")
      bw.newLine()

      Thread.sleep(500)
      line = br.readLine()
    }
    br.close()
  }
}
