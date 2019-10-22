//package cn.doit.crawler.demo
//
//import org.jsoup.Jsoup
//
//object JsoupJdDemo {
//  def main(args: Array[String]): Unit = {
//    val doc = Jsoup.connect("https://list.jd.com/list.html?cat=670,671,672").get()
//    val strong = doc.select("strong.J_price")
//    println(strong)
//  }
//
//}
