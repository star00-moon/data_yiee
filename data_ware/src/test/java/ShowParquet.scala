//import cn.doitedu.commons.utils.SparkUtil
//import org.apache.log4j.{Level, Logger}
//
//object ShowParquet {
//
//
//  def main(args: Array[String]): Unit = {
//
//    Logger.getLogger("org").setLevel(Level.WARN)
//    val spark = SparkUtil.getSparkSession()
//
//    val path = "user_profile/demodata/idmp/output/day01";
//    val df = spark.read.parquet(path)/*.where("eventType='ad_show'")*/
//
//    df.printSchema()
//    df.show(100,false)
//
//    spark.close()
//
//  }
//}
