package cn.doitedu.course.demo

import cn.doitedu.commons.utils.SparkUtil
import org.apache.spark.sql.{Dataset, Encoders}

import scala.collection.mutable


/**
 * @date: 2019/8/30
 * @site: www.doitedu.cn
 * @author: hunter.d 涛哥
 * @qq: 657270652
 * @description:
  *  spark dataset中encoder问题
 */
object DatasetEncoder {

  def main(args: Array[String]): Unit = {

    val sparkSession = SparkUtil.getSparkSession()
    import sparkSession.implicits._


    // 在spark.implicits中含有（基本类型，caseclass类型）的encoder编码器
    val dsx: Dataset[String] = sparkSession.createDataset(Seq(
      "a",
      "b"
    ))

    dsx.show(10,false)



    // 指定一个自己的序列化Encoder来序列化我们的dataset中的非默认支持类型（基本类型、case class）
    val encoder = Encoders.kryo(classOf[Map[String,String]])
    val dsy: Dataset[Map[String, String]] = sparkSession.createDataset(Seq(
      Map("a"->"1","b"->"2"),
      Map("x"->"5","y"->"6")
    ))(encoder)

    val dsz: Dataset[String] = dsy.map(mp=>{
      mp.getOrElse("a","0")
    })

    dsz.show(10,false)

    sparkSession.close()
  }

}
