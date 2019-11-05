package cn.doitedu.profile.idmp

import java.io.File

import cn.doitedu.commons.utils.{FileUtils, IdsExtractor, SparkUtil}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.graphx.{Edge, Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
  * @author: 余辉
  * @blog: https://blog.csdn.net/silentwolfyh
  * @create: 2019/10/22
  * @description: 常规的id映射字典计算程序
  **/
object RegularIdmp {

  def main(args: Array[String]): Unit = {
    //1、建立session连接
    Logger.getLogger("org").setLevel(Level.WARN)
    val spark: SparkSession = SparkUtil.getSparkSession(this.getClass.getSimpleName)
    import spark.implicits._

    //2、定义处理数据路径 cmccLogPath dspLogPath eventLogPath 以及 抽取当日的各类数据的id标识数据
    // 2-1、测试数据
//    val cmccLogPath = "user_profile/demodata/idmp/input/day02/cmcclog"
//    val dspLogPath = "user_profile/demodata/idmp/input/day02/dsplog"
//    val eventLogPath = "user_profile/demodata/idmp/input/day02/eventlog"
//    val cmccIds: RDD[Array[String]] = IdsExtractor.extractDemoCmccLogIds(spark, cmccLogPath)
//    val dspIds: RDD[Array[String]] = IdsExtractor.extractDemoDspLogIds(spark, dspLogPath)
//    val eventIds: RDD[Array[String]] = IdsExtractor.extractDemoEventLogIds(spark, eventLogPath)

    // 2-1、真实数据
        val cmccLogPath = "user_profile/data/cmcclog/day01"
        val dspLogPath = "user_profile/data/dsplog/day01"
        val eventLogPath = "user_profile/data/eventlog/day01"
        val cmccIds: RDD[Array[String]] = IdsExtractor.extractCmccLogIds(spark, cmccLogPath)
        val dspIds: RDD[Array[String]] = IdsExtractor.extractDspLogIds(spark, dspLogPath)
        val eventIds: RDD[Array[String]] = IdsExtractor.extractEventLogIds(spark, eventLogPath)

    // 3、查询cmccIds、dspIds、eventIds的并集
    // union是返回两个数据集的并集，不包括重复行，要求列数要一样，类型可以不同
    val ids: RDD[Array[String]] = cmccIds.union(dspIds).union(eventIds)

    // 4、映射点集合
    val vertices: RDD[(Long, String)] = ids.flatMap(arr => {
      arr.map(strId => (strId.hashCode.toLong, strId))
    })

    // 5、 映射边集合
    val edges: RDD[Edge[String]] = ids.flatMap(arr => {
      val sortedArr: Array[String] = arr.sorted
      for (i <- 0 until sortedArr.size - 1; j <- i + 1 until sortedArr.size) yield {
        Edge(sortedArr(i).hashCode.toLong, sortedArr(j).hashCode.toLong, "")
      }
    })

    // 6、过滤出现次数<阈值的边
    val filteredEdges: RDD[Edge[String]] = edges.map(edge => (edge, 1)).reduceByKey(_ + _).filter(_._2 > 0).map(_._1)

    //  7、加载上日的idmapping数据
    val idmpDF: DataFrame = spark.read.parquet("user_profile/demodata/idmp/output/day01")

    // 8、将上日的idmp数据也映射成点集合和边集合
    val preIds: RDD[(VertexId, String)] = idmpDF.rdd.flatMap({
      case Row(id: Long, gid: Long) => Array((id, ""), (gid, ""))
    })
    val preEges: RDD[Edge[String]] = idmpDF.rdd.map({
      case Row(id: Long, gid: Long) => Edge(id, gid, "")
    })

    // 9、历史点集合+今日点集合， 历史边集合+今日边集合 ==》 构造图
    val graph = Graph(vertices.union(preIds), filteredEdges.union(preEges))

    // 10.求最大连通子图    id_x->gid
    val currentDayResult: VertexRDD[VertexId] = graph.connectedComponents().vertices

    // 利用上日的IDmapping来调整今日的计算结果（保持gid的延续性）  先将历史idmapping数据收集到driver端为hashmap结构，并广播
    val idmpMap: collection.Map[VertexId, VertexId] = idmpDF.rdd.map({
      case Row(id: Long, gid: Long) => (id, gid)
    }).collectAsMap()
    val bc: Broadcast[collection.Map[VertexId, VertexId]] = spark.sparkContext.broadcast(idmpMap)

    // 11、利用上日idmap调整当日计算结果
    val ajustedResult: RDD[(VertexId, VertexId)] = currentDayResult
      // 11-1、按今日gid分组 ,分组之后变为（gid,(id1,id2,id3 。。。 )）
      .groupBy(_._2)
      .flatMap(tp => {
        // 11-2、取到一组ids的今日计算结果gid
        var gid: VertexId = tp._1

        // 11-3、取到一组ids
        val idSet: Set[VertexId] = tp._2.toMap.keySet

        // 11-4、从广播变量中取所有历史id
        val idmpLastDay: collection.Map[VertexId, VertexId] = bc.value
        val historyIdSet: collection.Set[VertexId] = idmpLastDay.keySet

        // 11-5、取历史idset和今日这一组idset的交集
        val sharedIds: Set[VertexId] = idSet.intersect(historyIdSet)

        //  11-6、如果存在交集，则取历史的gid作为当日这组ids的gid
        if (sharedIds != null && sharedIds.nonEmpty) {
          gid = idmpLastDay(sharedIds.head)
        }

        // 11-7、再返回这一组结果
        idSet.map(id => (id, gid))

      })

    // 12、输出保存最终结果
    FileUtils.deleteDir(new File("user_profile/data/output/idmp/day01"))
    ajustedResult.toDF("id", "gid").coalesce(1).write.parquet("user_profile/data/output/idmp/day01")
    ajustedResult.toDF("id", "gid").show(100, false)

    // 13、关闭spark
    spark.close()

    /***
      * +-----------+-----------+
      * |id         |gid        |
      * +-----------+-----------+
      * |722942308  |149045689  |
      * |149045689  |149045689  |
      * |1658657629 |149045689  |
      * |1915634795 |149045689  |
      * |-982955481 |3533212    |
      * |3533212    |3533212    |
      * |149045687  |3533212    |
      * |1915634793 |3533212    |
      * |-1081811326|3533212    |
      * |1658657627 |3533212    |
      * |722942306  |3533212    |
      * |1915634794 |3533213    |
      * |722942307  |3533213    |
      * |1658657628 |3533213    |
      * |3533213    |3533213    |
      * |1860147326 |3533213    |
      * |-1081811322|-1081811323|
      * |-1081811292|-1081811323|
      * |3533216    |-1081811323|
      * |-1081811323|-1081811323|
      * |1915634798 |-1081811323|
      * |149045691  |-1081811323|
      * |3533215    |-1081811323|
      * |-982955478 |-1081811323|
      * |1915634797 |-1081811323|
      * |-1081811321|-1081811323|
      * |149045692  |-1081811323|
      * |3533217    |-1081811323|
      * |-982955476 |-1081811323|
      * |-982955477 |-1081811323|
      * |1915634796 |-1081811323|
      * |149045690  |-1081811323|
      * +-----------+-----------+
      */
  }
}
