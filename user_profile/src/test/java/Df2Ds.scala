import cn.doitedu.commons.beans.EventLogBean
import cn.doitedu.commons.utils.SparkUtil
import org.apache.spark.sql.Dataset

/**
 * @date: 2019/9/19
 * @site: www.doitedu.cn
 * @author: hunter.d 涛哥
 * @qq: 657270652
 * @description:  dataframe  转 dataset
 */
case class GoodsInfo(skuid:String,name:String,brand:String,c1:String,c2:String,c3:String)

object Df2Ds {
  def main(args: Array[String]): Unit = {
    val spark = SparkUtil.getSparkSession("")
    import spark.implicits._

    // 读取parquet
    val df = spark.read.parquet("user_profile/data/output/eventlog/day01")

    val ds: Dataset[EventLogBean] = df.as[EventLogBean]

    ds.map(bean=>{
      (bean.gid,bean.event)
    })
      .toDF("gid","event")
      .show(10,false)

    // 读取csv
    val df2 = spark.read.option("header",true).csv("user_profile/data/t_goods/goods_info.csv")
    val ds2: Dataset[GoodsInfo] = df2.as[GoodsInfo]
    ds2.map(bean=>{
      (bean.skuid,bean.brand)
    }).toDF("skuid","brand")
      .show(10,false)

    spark.close()
  }

}
