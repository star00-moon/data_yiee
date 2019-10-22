package cn.doitedu.course.demo

import cn.doitedu.commons.utils.SparkUtil
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.expressions.Window


/**
  * @author: 余辉
  * @blog: https://blog.csdn.net/silentwolfyh
  * @create: 2019/10/22
  * @description: DSL ： 特定领域语言
  *               (将sql语法中的关键字和sql函数，在程序中像调程序的方法一样调用）
  **/

case class Person(id: Int, name: String)

case class Student(id: Int, name: String, age: Int)


object DSLDemo {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.WARN)

    val spark = SparkUtil.getSparkSession()

    val df = spark.read.csv("data_ware/demodata/score3.txt").toDF("id", "name", "gender", "score")
    df.cache()

    df.printSchema()
    df.show(10, false)
    df.orderBy("score").show(10, false)

    /**
      * sql中的关键字：
      *
      * select   field,function,constant, f1 oper f2
      *
      * from    join
      *
      * where
      *
      * group by
      *
      * having
      *
      * order by
      *
      * limit
      *
      */


    /**
      * select
      */


    // 1. 基本的select调用，使用字符串来标识字段
    df.select("id", "score")
      .show(10, false)

    // 2.基本的select调用，使用Column来标识字段
    df.select(df("id"), df("name"))
      .show(10, false)


    // 3.基本的select调用，使用Column来标识字段，但不需要用dataframe的apply方法来构造column对象，用别名：$""
    import spark.implicits._
    df.select($"id", $"name")

    // 4.基本的select调用，使用Column来标识字段，但不需要用dataframe的apply方法来构造column对象，用别名: 单边的单引号
    df.select('id, 'name)


    // 5.基本的select调用，使用Column来标识字段，但不需要用dataframe的apply方法来构造column对象，用sparksql的内置函数col来构造column对象
    import org.apache.spark.sql.functions._
    df.select(col("id"), col("score"))


    // 在select中用column来表示列，有好处：可以针对column应用各种sql函数
    df.select('id, upper('name)) // DSL风格
    // 当然，不用这种调程序方法的方式，也可以用sql字符串的方式来应用函数
    // 诀窍：使用selectExpr，可以传入各种sql表达式！
    df.selectExpr("id", "upper(name)") // SQL风格


    /**
      * where
      */

    /**
      * select *
      * from
      * (
      * select id,name
      * from t  where score>80
      * ) o
      * where o.name like 'z%'
      */
    // where使用方式1：用字符串布尔表达式来表示过滤条件
    val x = df.where("score > '80'")
      .select("id", "name")
    x.show(10, false)

    x.where("name like 'z%'")
      .show(10, false)

    // 用column来表示过滤条件（纯DSL风格）
    // 各种在column上调用的运算符，可以在@ org.apache.spark.sql.Column类中查看
    /**
      * @org.apache.spark.sql.Column
      */
    df.where('score <=> "80")
      .show(10, false)


    /**
      * group by
      */

    // 1. group by 的基本使用
    df.selectExpr("id", "name", "gender", "cast(score as double) as score")
      .groupBy("gender")
      .max("score").withColumnRenamed("max(score)", "score")
      .show(10, false)

    df.selectExpr("id", "name", "gender", "cast(score as double) as score")
      .groupBy("gender")
      .agg("score" -> "max", "score" -> "min", "score" -> "sum")
      .toDF("gender", "max_sc", "min_sc", "sum_sc") // 对全表所有字段重命名
      .withColumnRenamed("sum_sc", "sum") // 对指定字段重命名
      .show(10, false)

    // select gender,sum(),max(),min(),avg() from t group by gender
    df.selectExpr("id", "name", "gender", "cast(score as double) as score")
      .groupBy("gender")
      .agg('gender, min('score), max("score"), sum($"score"))


    /**
      * DSL的窗口分析函数
      * 需求： 求每个性别中分数最高的人的信息
      */
    // 用sql表达式来写
    df.selectExpr("id", "name", "gender", "score", "row_number() over(partition by gender order by score desc)  as rn ")
      .where('rn === 1)
      .show(10, false)

    // 用纯DSL风格来写
    val window = Window.partitionBy('gender).orderBy('score.desc)
    df.select('id, 'name, 'gender, 'score, row_number().over(window).as("rn"))
      .where('rn === 1)
      .show(10, false)


    /**
      * DSL风格的join
      */
    val stu = spark.read.option("header", "true").csv("data_ware/demodata/stu.txt")

    // 笛卡尔积
    df.crossJoin(stu)
      .show(10, false)


    // 最基本的join条件写法，用usingColumn="列名"，表示： 左表的该列=右表的该列
    // 连接字段在结果中只保留一列
    df.join(stu, "id")
      .show(10, false)

    // 左右表的连接字段在结果中都会出现
    df.join(stu.withColumnRenamed("id", "id2"), df("id") === stu("stuid"))
      .show(10, false)


    // 指定多个连接列，并指定连接类型：左连接 left，右连接right，内连接inner，全外连接full ,left semi左半连接
    df.join(stu, Seq("id"), "left")
      .show(10, false)

    /**
      * JoinWith ，是针对dataset的特定join，可以保留dataset中原有的数据类型
      */
    val ds1 = spark.createDataset(Seq(Person(1, "zs"), Person(2, "ls")))
    val ds2 = spark.createDataset(Seq(Student(1, "zs", 28), Student(2, "ls", 35)))

    // 仔细对照下面两句话，你就能体会到join和joinwith的区别
    val dsx: DataFrame = ds1.join(ds2, ds1("id") === ds2("id"))
    val dsy: Dataset[(Person, Student)] = ds1.joinWith(ds2, ds1("id") === ds2("id"))
    dsx.show(10, false)
    dsy.show(10, false)


    spark.close()

  }


}
