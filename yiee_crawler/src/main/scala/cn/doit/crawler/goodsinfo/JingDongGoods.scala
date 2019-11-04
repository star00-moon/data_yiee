package cn.doit.crawler.goodsinfo

import java.io.{BufferedWriter, FileWriter}

import org.jsoup.Jsoup
import org.jsoup.nodes.{Document, Element}
import org.jsoup.select.Elements

/**
  * @author: 余辉
  * @blog: https://blog.csdn.net/silentwolfyh
  * @create: 2019/10/22
  * @description: 京东葡萄酒类商品信息抓取程序，保存【商品标题】和【商品详情页地址】
  **/
object JingDongGoods {
  def main(args: Array[String]): Unit = {
    fetchGoodsBaseInfo("yiee_crawler/data/jdgoods/goods.jd")
  }

  /**
    * 获取京东商品基本信息，保存【商品标题】和【商品详情页地址】
    */
  def fetchGoodsBaseInfo(savepath: String): Unit = {

    //1、配置headers文件
    val userAgent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.157 Safari/537.36"
    val headers = Map("userAgent" -> userAgent, "referal" -> "http://www.baidu.com")

    import scala.collection.JavaConversions._

    //2、建立IO流，写入数据
    val bw = new BufferedWriter(new FileWriter(savepath, true))

    //3、循环1到10页
    for (i <- 1 to 10) {

      //3-1、获取URL
      val url = s"https://list.jd.com/list.html?cat=12259,14714&page=${i}"

      //3-2、通过Jsoup请求页面，返回值
      val doc: Document = Jsoup.connect(url).headers(headers).execute().parse()

      //3-3、获取每页class为"gl-item"
      val liList: Elements = doc.getElementsByClass("gl-item")
      for (li <- liList) {
        //3-4、获取class为“p-name”的数据
        val div: Element = li.getElementsByClass("p-name").get(0)
        //3-5、获取链接
        val atag: Elements = div.getElementsByTag("a")
        //3-6、提取商品标题
        val pname: String = atag.text()
        //3-7、提取商品详情页地址
        val purl: String = atag.attr("href")
        println(s"酒类|葡萄酒\001${pname}\001${purl}")
        bw.write(s"酒类|葡萄酒\001${pname}\001https:${purl}")
        bw.newLine()
        Thread.sleep(100)
      }
    }

    //4、IO流 刷新，关闭
    bw.flush()
    bw.close()
  }
}



