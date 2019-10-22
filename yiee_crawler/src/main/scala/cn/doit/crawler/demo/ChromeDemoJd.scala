package cn.doit.crawler.demo

import org.openqa.selenium.WebElement
import org.openqa.selenium.chrome.{ChromeDriver, ChromeOptions}

object ChromeDemoJd {
  def main(args: Array[String]): Unit = {

    System.getProperties.setProperty("webdriver.chrome.driver","C:\\Work\\soft\\chromedriver.exe")

    val options = new ChromeOptions()
    options.addArguments("headless")

    val driver = new ChromeDriver(options)

    driver.get("https://www.jd.com")
    driver.get("https://list.jd.com/list.html?cat=670,671,672")

    import scala.collection.JavaConversions._
    //val lst1  = driver.findElementsByCssSelector("div.p-price > i")
    val lst2 = driver.findElementsByCssSelector("div.p-price i")

    //lst1.foreach(e=>println(e))
    println("---------------------------------")
    lst2.foreach(e=>println(e.getText))
  }
}
