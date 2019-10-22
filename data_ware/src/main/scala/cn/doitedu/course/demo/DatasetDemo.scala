package cn.doitedu.course.demo

import cn.doitedu.commons.utils.SparkUtil
import org.apache.spark.sql.{DataFrame, Dataset, Row}

case class Stu(id:Int,name:String,gender:String,score:Double)


object DatasetDemo {


  def main(args: Array[String]): Unit = {


    val stu = Stu(1,"zs","M",80.98)

    val stu_map: Map[String, Any] = Map("id"->1,"name"->"zs","gender"->"M","score"->80.98)


    val spark = SparkUtil.getSparkSession()
    import spark.implicits._

    /*val df = spark.read.option("header","true").csv("data_ware/demodata/score3.txt")

    df.rdd.map(row=>{

      val id = row.getAs[Int]("id")  // 强转，编译时无法检查是否错误，一旦类型不对，运行时会强转失败抛异常

    })*/



    val ds: Dataset[String] = spark.read.textFile("data_ware/demodata/score3.txt")
    // dataset中的元素的类型可以是任何用户自定义类型，比dataframe的类型要丰富的多
    // dataframe中的元素只有一种：row，你要用dataframe，那么你的任意数据都必须封装到row中

    val ds2: Dataset[Stu] = ds.map(str=>{
      val arr = str.split(",")
      Stu(arr(0).toInt,arr(1),arr(2),arr(3).toDouble)
    })

    // dataset可以自动反射出表结构（从case class元素中反射）
    ds2.printSchema()

    // 既然dataset带schema，它也拥有了sql查询的功能
    ds2.select("id","name")
      .show(10,false)


    // 万一不做sql查询，需要用map等算子来进行底层处理，它是强类型的
    /*ds2.map(stu=>{
      // val gender:Double= stu.gender  类型不对，直接编译就报错
      val id: Int = stu.id
    })*/

    // dataset经过select运算后，得到的结果就不一定符合原来的特定类型结构了，所以又会转成通用结构：Row
    val ds3: Dataset[Row] = ds2.select("id","socre")


    spark.close()

  }
}
