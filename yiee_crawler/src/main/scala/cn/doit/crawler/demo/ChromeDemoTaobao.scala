//package cn.doit.crawler.demo
//
//import org.openqa.selenium.chrome.ChromeDriver
//
//object ChromeDemoTaobao {
//
//  def main(args: Array[String]): Unit = {
//
//    System.getProperties.setProperty("webdriver.chrome.driver","C:\\Users\\coder\\AppData\\Local\\Google\\Chrome\\Application\\chromedriver.exe")
//    val driver = new ChromeDriver()
//
//    driver.get("https://login.taobao.com/member/login.jhtml?spm=a21bo.2017.754894437.1.5af911d9lioaPi&f=top&redirectURL=https%3A%2F%2Fwww.taobao.com%2F")
//
//    val loginFlag = driver.findElementByClassName("login-title").getText
//    Thread.sleep(5000)
//
//    val username = driver.findElementByCssSelector("a.site-nav-login-info-nick.super").getText
//    if(username.equals("gspbc_dht")){
//
//      //  就开始去爬我们的各种商品清单信息
//      driver.get("https://s.taobao.com/search?spm=a21bo.2017.201867-links-7.32.5af911d9iDhfsK&q=%E6%89%8B%E5%8A%9E&imgfile=&js=1&stats_click=search_radio_all%3A1&initiative_id=staobaoz_20190221&ie=utf8&cps=yes&cat=25&filter=reserve_price%5B200%2C%5D")
//
//      val lst = driver.findElementsByCssSelector(".ctx-box.J_MouseEneterLeave.J_IconMoreNew")
//      import scala.collection.JavaConversions._
//      lst.foreach(e=>println(e.getText))
//    }else{
//      println("没检测到登录成功标志")
//    }
//  }
//}
