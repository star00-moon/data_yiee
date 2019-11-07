package cn.doitedu.profile.idmp

import java.io.File

import cn.doitedu.commons.utils.{FileUtils, IdsExtractor, SparkUtil}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.{Edge, Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable


/**
  * @author: 余辉
  * @blog: https://blog.csdn.net/silentwolfyh
  * @create: 2019/10/22
  * @description: T日IDMP计算程序
  **/
object TdayIdmp {

  def main(args: Array[String]): Unit = {

    // 1、建立session连接  import spark.implicits._
    Logger.getLogger("org").setLevel(Level.WARN)
    val spark: SparkSession = SparkUtil.getSparkSession(this.getClass.getSimpleName)
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
    val ids: RDD[Array[String]] = cmccIds.union(dspIds).union(eventIds)
    ids.cache()

    //  5、映射点集合 vertices (strId.hashCode.toLong, strId)
    val vertices: RDD[(Long, String)] = ids.flatMap(arr => {
      arr.map(strId => (strId.hashCode.toLong, strId))
    })

    // 6、映射边集合 edges。1）每个array中的id需要提前排序；2）每行数据两两直接都要建立边，所以需要双层for循环。3）id都需要hashCode
    val edges: RDD[Edge[String]] = ids.flatMap(arr => {
      val sortedArr: Array[String] = arr.sorted
      for (i <- 0 until sortedArr.length - 1; j <- i + 1 until sortedArr.length) yield {
        Edge(sortedArr(i).hashCode.toLong, sortedArr(j).hashCode.toLong, sortedArr(i) + "--》" + sortedArr(j))
      }
    })

    // 7、过滤出现次数<阈值的边 , 相同边出现两次的过滤
    val filteredEdges: RDD[Edge[String]] = edges.map(edge => (edge, 1)).reduceByKey(_ + _).filter(_._2 > 2).map(_._1)

    // 8、构造图
    val graph = Graph(vertices, filteredEdges)

    // 9、求最大连通子图    id_x->gid
    val idAndGid: VertexRDD[VertexId] = graph.connectedComponents().vertices

    //  10、整理结果保存parquet格式
    FileUtils.deleteDir(new File("user_profile/demodata/idmp/output/day01"))
    idAndGid.toDF("id", "gid").coalesce(1).write.parquet("user_profile/demodata/idmp/output/day01")

    //  11、 关闭spark
    spark.close()

    /** *
      * +-----------+-----------+
      * |id         |gid        |
      * +-----------+-----------+
      * |-1081811323|-1081811323|
      * |1915634796 |-1081811323|
      * |1915634793 |3533212    |
      * |722942307  |3533213    |
      * |1658657628 |3533213    |
      * |1915634794 |3533213    |
      * |722942308  |149045689  |
      * |149045689  |149045689  |
      * |3533215    |-1081811323|
      * |3533212    |3533212    |
      * |1658657629 |149045689  |
      * |722942306  |3533212    |
      * |149045687  |3533212    |
      * |149045690  |-1081811323|
      * |-982955478 |-1081811323|
      * |3533213    |3533213    |
      * |1658657627 |3533212    |
      * |1860147326 |3533213    |
      * |1915634795 |149045689  |
      * +-----------+-----------+
      */
  }

}
