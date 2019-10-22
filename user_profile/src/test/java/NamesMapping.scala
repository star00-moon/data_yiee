import cn.doitedu.commons.utils.SparkUtil
import org.apache.log4j.{Level, Logger}

import scala.collection.immutable

/**
 * @author: 余辉 
 * @blog:   https://blog.csdn.net/silentwolfyh
 * @create: 2019/10/22
 * @description: 
 **/
object NamesMapping {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    val spark = SparkUtil.getSparkSession("")
    import spark.implicits._
    val df = spark.createDataset(Seq("18,zs,1", "20,ls,2"))
      .map(line => {
        val arr = line.split(",")
        (arr(0),arr(1),arr(2))
      }).toDF("age","name","id")

    df.show(10,false)


    val originColumns = Seq("age", "name", "id")
    val globalNames = Seq("stuid","stuname","stuage")
    val indices = Seq(2, 1, 0)
    df
      .map(row => {
        val tuples: Seq[(String, Int)] = originColumns.zip(indices).sortBy(_._2)
        val res: immutable.Seq[String] = for(i<- 0 until indices.size) yield row.getAs[String](indices(i))
        res match {
          case Seq(a,b,c) =>(a,b,c)
        }
      }).toDF(globalNames.toArray: _*)
      .show(10,false)


    spark.close()
  }

}
