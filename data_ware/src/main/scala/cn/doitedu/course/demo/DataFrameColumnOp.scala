package cn.doitedu.course.demo

import cn.doitedu.commons.utils.SparkUtil
import com.alibaba.fastjson.JSON
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.apache.spark.sql.{Dataset, Row}

/**
 * @date: 2019/8/28
 * @site: www.doitedu.cn
 * @author: hunter.d 涛哥
 * @qq: 657270652
 * @description:  dataframe的一些少见的操作
  *              列的添加和抛弃
 */
object DataFrameColumnOp {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)

    val spark = SparkUtil.getSparkSession()
    import spark.implicits._

    val ds = spark.createDataset(Seq(Student(1,"zs",28),Student(2,"ls",35)))
    val df = ds.toDF()
    df.printSchema()
    df.show(10,false)

    // 丢弃一列
    val df1 = df.select("id","name")  // 这是最基本的操作方式，但是如果列很多，而要丢弃的列很少，则代码太长

    val df2 = df.drop("age","name")


    // 往已存在的表（dataframe）中添加一列
    df.selectExpr("id","name","age","'北京'") // 最基本操作，多select出一个表达式

    // 方式1，从row中把所有字段都取出来
    df.map(row=>{
      val id = row.getAs[Int]("id")
      val name = row.getAs[String]("name")
      val age = row.getAs[Int]("age")

      // 根据name去查询一个字典表，然后得到一个结果，然后添加到数据中
      val x = "xxx"

      (id,name,age,x)
    })


    // 方式2，给row追加数据
    val rdd:RDD[Row] = df.rdd.map(row=>{

      // 从row中取出需要的字段（按需取）
      val name = row.getAs[String]("name")
      // 根据name去查询一个字典表，然后得到一个结果，然后添加到数据中
      val x: String = "xxx"

      // 把row中的数据值toSeq得到seq
      val values: Seq[Any] = row.toSeq
      // 给值seq添加新的数据元素
      val newvalues: Seq[Any] = values.:+(x)

      // 将组装好的数据值放入row
      Row(newvalues:_*)

    })

    val schema: StructType = df.schema
    val schemaNew = schema.add("x",DataTypes.StringType)

    val df3 = spark.createDataFrame(rdd,schemaNew)
    df3.printSchema()
    df3.show(10,false)

    spark.close()
  }

}
