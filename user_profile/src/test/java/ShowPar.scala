//import cn.doitedu.commons.utils.SparkUtil
//
//object ShowPar {
//
//  def main(args: Array[String]): Unit = {
//    val spark = SparkUtil.getSparkSession("")
//    val x = spark.read.option("header",true).csv("user_profile/demodata/tags/day01/eventtags")
//    x.show(10, false)
//    /*.select("gid")
//    .distinct()
//    .show(100,false)
//
//  spark.close()*/
//    import spark.implicits._
//
//    x.repartition(2,$"gid",$"tag_module")
//      .write.json("d:/xout")
//
//
//
//    spark.close()
//
//  }
//
//}
