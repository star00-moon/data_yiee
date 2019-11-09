package cn.doit.crawler.appinfo

import java.io.{BufferedReader, BufferedWriter, FileReader, FileWriter}

import org.jsoup.Jsoup
import org.jsoup.nodes.{Document, Element}
import org.jsoup.select.Elements

/**
  * @author: 余辉
  * @blog: https://blog.csdn.net/silentwolfyh
  * @create: 2019/10/22
  * @description: appchina应用下载市场app信息爬取程序
  *
  *               appchina也有一定的反爬措施：
  *               1. 监测客户端的useragent
  *               2. 监测同一个ip来的访问频率
  *               应对措施：设置useragent；
  *               不断更换代理服务器；
  *               适当降低请求频率；
  **/
object AppChina {

  /** *
    * 获取网页中的  appid appname appDtlUrl 保存到yiee_crawler/data/appchina-dtlurls/dtl.urls
    *
    * @param urlSavePath
    */
  def getAppDtlUrls(urlSavePath: String) = {
    // 1、建立头文件内容
    val userAgent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.157 Safari/537.36"
    val cookie = "Hm_lvt_c1a192e4e336c4efe10f26822482a1a2=1568770426; Hm_lpvt_c1a192e4e336c4efe10f26822482a1a2=1568770426; UM_distinctid=16d420103a3367-05aee9bc53c7c5-353166-1fa400-16d420103a4371; CNZZDATA1257961818=393573309-1568768131-null%7C1568768131"
    val headers: Map[String, String] = Map("userAgent" -> userAgent, "referal" -> "http://www.baidu.com", "Cookie" -> cookie)

    // 2、建立IO流  BufferedWriter(new FileWriter)
    val bw = new BufferedWriter(new FileWriter(urlSavePath, true))

    // 3、循环翻页
    for (i <- 24 to 34) {
      // 3-1、获取URL， import scala.collection.JavaConversions._
      val url = s"http://www.appchina.com/category/30/${i}_1_1_3_0_0_0.html"
      println(s"-----------${i}-----------")
      import scala.collection.JavaConversions._

      // 3-2、通过Jsoup请求页面，程序执行且获取网页内容
      val doc: Document = Jsoup.connect(url).headers(headers).execute().parse()

      // 3-3、获取所有 class 为 app-name的标签
      val h1tags: Elements = doc.getElementsByClass("app-name")

      // 3-4、获取基础数据进行循环获取
      for (h1 <- h1tags) {
        // 3-4-1、获取 title 的属性值 ，取名：appname
        val appname: String = h1.attr("title")

        // 3-4-2、获取第一个 a 便签 ， 取名：atag
        val atag: Element = h1.getElementsByTag("a").get(0)

        // 3-4-3、获取atag的属性值 href，取名：appDtlUrl
        val appDtlUrl: String = atag.attr("href")

        // 3-4-4、获取 appDtlUrl 的 appid
        val appid: String = appDtlUrl.substring(appDtlUrl.lastIndexOf("/") + 1)

        // 3-4-5、保存数据 appid 、appname 、appDtlUrl \001隔开
        println(s"${appid}\001${appname}\001${appDtlUrl}")
        bw.write(s"${appid}\001${appname}\001${appDtlUrl}")
        bw.newLine()

        Thread.sleep(500)
      }
    }
    //4、IO流 刷新，关闭
    bw.flush()
    bw.close()
  }


  /** *
    * 根据获取app清单，分别获取每个详细的app应用数据，保存数据为：app的id ， app名称 ， app相信信息
    *
    * @param dtlUrls
    * @param resultSavePath
    */
  def getAppDesc(dtlUrls: String, resultSavePath: String): Unit = {
    // 1、建立头文件内容
    val userAgent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.157 Safari/537.36"
    val cookie = "Hm_lvt_c1a192e4e336c4efe10f26822482a1a2=1568770426; Hm_lpvt_c1a192e4e336c4efe10f26822482a1a2=1568770426; UM_distinctid=16d420103a3367-05aee9bc53c7c5-353166-1fa400-16d420103a4371; CNZZDATA1257961818=393573309-1568768131-null%7C1568768131"
    val headers: Map[String, String] = Map("userAgent" -> userAgent, "referal" -> "http://www.baidu.com", "Cookie" -> cookie)

    // 2、建立IO流，BufferedWriter(new FileWriter)
    val bw = new BufferedWriter(new FileWriter(resultSavePath, true))

    // 3、加载详情页地址列表 BufferedReader(new FileReader(））
    val br = new BufferedReader(new FileReader("yiee_crawler/data/appchina-dtlurls/dtl.urls"))
    var line: String = br.readLine()

    // 4、循环遍历每一个地址，抓取详情页，提取app描述信息
    while (line != null) {
      // 4-1、获取AppURL，通过Jsoup请求页面，程序执行且获取网页内容
      val arr: Array[String] = line.split("\001")
      val url: String = arr(2)
      import scala.collection.JavaConversions._
      val doc: Document = Jsoup.connect(s"http://www.appchina.com${url}").headers(headers).execute().parse()

      // 4-2、获取所有 class 为 art-content 的标签，取第一值的内容
      val p: Element = doc.getElementsByClass("art-content").get(0)
      val appDesc: String = p.text()

      // 4-3、保存app的id ， app名称 ， app相信信息，且按照 \001隔开
      println(s"${arr(0)}\001${arr(1)}\001${appDesc}")
      bw.write(s"${arr(0)}\001${arr(1)}\001${appDesc}")

      // 4-4、 sleep（500），bw新加载一条，br读取下一条传给初始值
      Thread.sleep(500)
      bw.newLine()
      line = br.readLine()
    }
    //5、IO流 刷新，关闭
    br.close()
  }

  def main(args: Array[String]): Unit = {
    // 获取网页中的  appid appname appDtlUrl
    getAppDtlUrls("yiee_crawler/data/appchina-dtlurls/dtl.urls")

    // 根据获取app清单，分别获取每个详细的app应用数据，保存数据为：app的id ， app名称 ， app相信信息
    getAppDesc("yiee_crawler/data/appchina-dtlurls/dtl.urls", "yiee_crawler/data/appdesc/app.desc")
  }
}
