package cn.doit.profile.dw

import java.util.Properties

import cn.doitedu.commons.utils.SparkUtil
import org.apache.spark.sql.SaveMode


/**
 * @author: 余辉
 * @blog:   https://blog.csdn.net/silentwolfyh
 * @create: 2019/10/22
 * @description:  DSP广告竞价业务kpi指标统计
 **/
object DspKpi {

  def main(args: Array[String]): Unit = {

    // 1、建立session连接  import spark.implicits._
    val spark = SparkUtil.getSparkSession(this.getClass.getSimpleName)
    import spark.implicits._

    // 2、加载数据 Creates a local temporary view
    val df = spark.read.parquet("user_profile/data/output/dsplog/day01")
    df.createTempView("dsp")

    // 3、编写Sql查询数据
    val res = spark.sql(
      """
        |
        |select
        |'2019-06-16' as dt,
        |provincename,
        |cityname,
        |appname,ispname,
        |sum(origin_req) as origin_req,
        |sum(effective_req) as effective_req,
        |sum(ad_req) as ad_req,
        |sum(bid_req) as bid_req,
        |sum(win_req) as win_req,
        |sum(ad_show) as ad_show,
        |sum(ad_click) as ad_click,
        |sum(costume) as costume,
        |sum(adpayment) as adpayment
        |from
        |(
        |  select
        |     provincename,
        |     cityname,
        |     appname,
        |     ispname,
        |     if(requestmode=1 and processnode>=1,1,0) as origin_req,
        |     if(requestmode=1 and processnode>=2,1,0) as effective_req,
        |     if(requestmode=1 and processnode=3,1,0) as ad_req,
        |     if(iseffective=1 and isbilling=1 and isbid=1 and adorderid!=0,1,0) as bid_req,
        |     if(iseffective=1 and isbilling=1 and iswin=1,1,0) as win_req,
        |     if(requestmode=2 and iseffective=1,1,0) as ad_show,
        |     if(requestmode=3 and iseffective=1,1,0) as ad_click,
        |     if(iseffective=1 and isbilling=1 and iswin=1,winprice/1000,0) as costume,
        |     if(iseffective=1 and isbilling=1 and iswin=1,adpayment/1000,0) as adpayment
        |  from dsp
        |) o
        |group by provincename,cityname,appname,ispname
        |grouping sets((provincename,cityname),(appname),(ispname))
        |
      """.stripMargin)

    res.show(10,false)

    // 4、将spark的计算结果dataframe写入报表库：mysql
    val props = new Properties()
    props.setProperty("user","root")
    props.setProperty("password","root")
    res.write.mode(SaveMode.Append).jdbc("jdbc:mysql://localhost:3306/yieerpts?characterEncoding=utf-8","dsp_kpi",props)

    spark.close()

  }

}
