package cn.doitedu.course.demo

import cn.doitedu.commons.utils.SparkUtil
import org.apache.spark.rdd.RDD

/**
 * @author: 余辉
 * @blog:   https://blog.csdn.net/silentwolfyh
 * @create: 2019/10/22
 * @description: RDD的基本操作入门
 **/
object RddOperatorDemo {

  def main(args: Array[String]): Unit = {

    val spark = SparkUtil.getSparkSession()

    // 如何创建一个RDD
    // 本质是：讲一个rdd之外的数据集（一个本地集合，一个hdfs上的文件，一个mysql中的表，一个hbase中的表........），映射成rdd（rdd就是一个“分布式”集合）
    val rdd: RDD[String] = spark.sparkContext.textFile("data_ware/demodata/a.txt")

    /**
      * rdd就是一个集合，对rdd的操作极其类似于对普通集合的操作
      */
    val upperRDD = rdd.map(str=>str.toUpperCase)


    /**
      * 普通集合的操作
      */
    val lst:List[String] = List(
      "hello tom hello kitty",
      "hello jim hello rose",
      "hello spark spark taoge"
    )


    var upperLst:List[String] = List.empty

    for (elem <- lst) {
      val upper = elem.toUpperCase()
      upperLst.+:(upper)
    }

    upperLst = lst.map(str=>str.toUpperCase())


  }

}
