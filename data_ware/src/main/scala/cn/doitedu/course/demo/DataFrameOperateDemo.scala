package cn.doitedu.course.demo

import cn.doitedu.commons.utils.SparkUtil
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types.{DataTypes, IntegerType, StructType}

/**
 * @date: 2019/8/25
 * @site: www.doitedu.cn
 * @author: hunter.d 涛哥
 * @qq: 657270652
 * @description:  dataframe 基本操作
 */
object DataFrameOperateDemo {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.WARN)

    val spark = SparkUtil.getSparkSession()



    /*
    * 结构化数据按非结构化数据处理，丧失了结构化数据的便利特性
    val rdd = spark.sparkContext.textFile("data_ware/demodata/score.txt")
    rdd.map(line=>line.split(","))
      .map(arr=>arr(2).toDouble)
      .sum()*/

    /**
      * dataframe 就是为结构化数据而生！！！
      * 可以充分利用结构化数据的结构特性，降低逻辑开发的复杂性
      * dataframe本质上还是rdd，只不过它对rdd进行了增强，在rdd上添加了“数据描述--表结构”,schemaRDD
      * 那么，dataframe的结构是在哪里描述的呢？在rdd的Row中的描述
      * 也就是说，dataframe的rdd中装的数据，不再是string，不再是array，不再是... ，而是Row,row中有这一行数据的每个数据的位置、类型信息、名称
      */

    // 将一个带有表头的csv文件映射成dataframe
    val df = spark.read.option("header","true").csv("data_ware/demodata/score.txt")
    df.show(10,false)



    // 将一个没有表头的csv文件映射成dataframe并在映射好之后更改字段名
    val df2 = spark.read/*.option("header","false")*/.csv("data_ware/demodata/score.txt")
    df2.printSchema()

    /**
      * root
      * |-- _c0: string (nullable = true)
      * |-- _c1: string (nullable = true)
      * |-- _c2: string (nullable = true)
      */
    df2.show(10,false)

    /**
      * +---+---+----+
      * |_c0|_c1|_c2 |
      * +---+---+----+
      * |1  |zs |80.8|
      * |2  |ls |90.6|
      * |3  |ww |66.6|
      * +---+---+----+
      */

    // 自己对字段更名
    val df3 = df2.toDF("id","name","sc")
    df3.printSchema()
    df3.show(10,false)


    // 将一个没有表头的csv文件映射成dataframe，并按照我们的预定要求映射表结构（字段名、字段类型）
    // 先自定义一个表结构，含有3个字段，分别叫id/name/score，类型分别为：int/string/double
    val schema = new StructType()
        .add("id",DataTypes.IntegerType)
        .add("name",DataTypes.StringType)
        .add("score",DataTypes.DoubleType)

    val df4 = spark.read.schema(schema).csv("data_ware/demodata/score.txt")
    println("----------df4--------------------------------------------")
    df4.printSchema()

    /**
      * root
      * |-- id: integer (nullable = true)
      * |-- name: string (nullable = true)
      * |-- score: double (nullable = true)
      */
    df4.show(10,false)


    df4.createTempView("df4")

    spark.sql(
      """
        |
        |select
        |sum(score)
        |
        |from df4
      """.stripMargin)
        .show(10,false)

    //import spark.implicits._
    // 如果有些逻辑不好用sql表达，那么dataframe也支持按rdd的方式去处理
    df4.rdd.map(row=>{
      val id = row.getAs[Int]("id")
      val name = row.getAs[String]("name")
      val score = row.getAs[Double]("score")
      // 数据从row中取出后，就爱怎么处理怎么处理了
    })



    spark.close()


  }

}
