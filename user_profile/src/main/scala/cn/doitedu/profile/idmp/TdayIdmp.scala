package cn.doitedu.profile.idmp

import java.io.File

import cn.doitedu.commons.utils.{FileUtils, IdsExtractor, SparkUtil}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.rdd.RDD


/**
  * @date: 2019/9/15
  * @site: www.doitedu.cn
  * @author: hunter.d 涛哥
  * @qq: 657270652
  * @description: T日IDMP计算程序
  */
object TdayIdmp {

  def main(args: Array[String]): Unit = {

    // 1、建立session连接  import spark.implicits._
    Logger.getLogger("org").setLevel(Level.WARN)
    val spark = SparkUtil.getSparkSession(this.getClass.getSimpleName)
    import spark.implicits._

    // 2、定义处理数据路径 cmccLogPath dspLogPath eventLogPath
    val cmccLogPath = "user_profile/demodata/idmp/input/day01/cmcclog";
    val dspLogPath = "user_profile/demodata/idmp/input/day01/dsplog";
    val eventLogPath = "user_profile/demodata/idmp/input/day01/eventlog";

    //  3、抽取3类数据中的id字段 , 按照逗号切分，过滤空字符串
    val cmccIds: RDD[Array[String]] = IdsExtractor.extractDemoCmccLogIds(spark, cmccLogPath)
    val dspIds: RDD[Array[String]] = IdsExtractor.extractDemoCmccLogIds(spark, dspLogPath)
    val eventIds: RDD[Array[String]] = IdsExtractor.extractDemoCmccLogIds(spark, eventLogPath)

    // 4、把3类整理好的id数据进行union整合到一起，缓存起来
    val ids = cmccIds.union(dspIds).union(eventIds)
    ids.cache()

    //  5、映射点集合 vertices
    val vertices: RDD[(Long, String)] = ids.flatMap(arr => {
      arr.map(strId => (strId.hashCode.toLong, strId))
    })

    // 6、映射边集合 edges
    val edges: RDD[Edge[String]] = ids.flatMap(arr => {
      val sortedArr = arr.sorted
      for (i <- 0 until sortedArr.size - 1; j <- i + 1 until sortedArr.size) yield {
        Edge(sortedArr(i).hashCode.toLong, sortedArr(j).hashCode.toLong, "")
      }
    })

    // 7、过滤出现次数<阈值的边 , 相同边出现两次的过滤
    val filteredEdges = edges.map(edge => (edge, 1)).reduceByKey(_ + _).filter(_._2 > 2).map(_._1)

    // 8、构造图
    val graph = Graph(vertices, filteredEdges)

    // 9、求最大连通子图    id_x->gid
    val result = graph.connectedComponents().vertices

    //  10、整理结果保存
    FileUtils.deleteDir(new File("user_profile/demodata/idmp/output/day01"))
    result.toDF("id", "gid").coalesce(1).write.parquet("user_profile/demodata/idmp/output/day01")

    result.toDF("id", "gid").show(100, false)

    result.toDF("id", "gid")

    spark.close()

  }

}
