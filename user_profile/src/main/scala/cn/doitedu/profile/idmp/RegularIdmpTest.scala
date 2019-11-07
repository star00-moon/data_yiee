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
object RegularIdmpTest {

  def main(args: Array[String]): Unit = {
    //1、建立session连接
    Logger.getLogger("org").setLevel(Level.WARN)
    val spark: SparkSession = SparkUtil.getSparkSession(this.getClass.getSimpleName)
    import spark.implicits._

    //2、定义处理数据路径 cmccLogPath dspLogPath eventLogPath 以及 抽取当日的各类数据的id标识数据
    // 2-1、测试数据
    val cmccLogPath = "user_profile/demodata/idmp/input/day02/cmcclog"
    val dspLogPath = "user_profile/demodata/idmp/input/day02/dsplog"
    val eventLogPath = "user_profile/demodata/idmp/input/day02/eventlog"
    val cmccIds: RDD[Array[String]] = IdsExtractor.extractDemoCmccLogIds(spark, cmccLogPath)
    val dspIds: RDD[Array[String]] = IdsExtractor.extractDemoDspLogIds(spark, dspLogPath)
    val eventIds: RDD[Array[String]] = IdsExtractor.extractDemoEventLogIds(spark, eventLogPath)

    // 2-1、真实数据
    //    val cmccLogPath = "user_profile/data/cmcclog/day01"
    //    val dspLogPath = "user_profile/data/dsplog/day01"
    //    val eventLogPath = "user_profile/data/eventlog/day01"
    //    val cmccIds: RDD[Array[String]] = IdsExtractor.extractCmccLogIds(spark, cmccLogPath)
    //    val dspIds: RDD[Array[String]] = IdsExtractor.extractDspLogIds(spark, dspLogPath)
    //    val eventIds: RDD[Array[String]] = IdsExtractor.extractEventLogIds(spark, eventLogPath)

    // 3、查询cmccIds、dspIds、eventIds的并集 ， union是返回两个数据集的并集，不包括重复行，要求列数要一样，类型可以不同
    val ids: RDD[Array[String]] = cmccIds.union(dspIds).union(eventIds)

    // 4、映射点集合 val vertices: RDD[(Long, String)]
    val vertices: RDD[(VertexId, String)] = ids.flatMap(row => {
      row.map(id => (id.hashCode.toLong, id))
    })

    // 5、 映射边集合 edges val edges: RDD[Edge[String]]
    val edges: RDD[Edge[String]] = ids.flatMap(arr => {
      val sortedArr: Array[String] = arr.sorted
      for (i <- 0 until sortedArr.size - 1; j <- i + 1 until sortedArr.size) yield {
        Edge(sortedArr(i).hashCode.toLong, sortedArr(j).hashCode.toLong, "")
      }
    })

    // 6、过滤出现次数<阈值的边
    val filterEdges: RDD[Edge[String]] = edges.map(e => (e, 1)).reduceByKey(_ + _).filter(_._2 > 1).map(r => r._1)

    //  7、加载上日的idmapping数据
    val yesterdayIDMapping: DataFrame = spark.read.parquet("user_profile/demodata/idmp/output/day01")

    // 8、将上日的idmp数据也映射成点集合和边集合
    // 8-1、点集合转为 Array((id, ""), (gid, ""))
    val yesterdayV: RDD[(VertexId, String)] = yesterdayIDMapping.rdd.flatMap({ case Row(id: VertexId, gid: VertexId) => Array((id, ""), (gid, "")) })
    // 8-2、边集合转为 Edge(id, gid, "")
    val yesterdayE: RDD[Edge[String]] = yesterdayIDMapping.rdd.map({ case Row(id: VertexId, gid: VertexId) => Edge(id, gid, "") })

    // 9、历史点集合union今日点集合， 历史边集合union今日边集合
    val graph = Graph(yesterdayV.union(vertices), yesterdayE.union(edges))

    // 10.求最大连通子图   currentDayResult
    val currentDayResult: VertexRDD[VertexId] = graph.connectedComponents().vertices

    // 11、利用上日idmapping调整今日的计算结果（保持gid的延续性），将历史的idmapping变成（id-->gid），转为map集合放入广播变量
    val idmpMap: collection.Map[VertexId, VertexId] = yesterdayIDMapping.rdd.map({ case Row(id: VertexId, gid: VertexId) => (id, gid) }).collectAsMap()
    val bc: Broadcast[collection.Map[VertexId, VertexId]] = spark.sparkContext.broadcast(idmpMap)

    // 12、利用上日idmapping调整今日的计算结果，所以需要今日的 currentDayResult 进行处理
    val ajustedResult: RDD[(VertexId, VertexId)] = currentDayResult
      // 12-1、按今日gid分组 【.groupBy(_._2)】,分组之后变为（gid,(id1,id2,id3 。。。 )）
      .groupBy(_._2)
      .flatMap(row => {
        // 12-2、取到今日的 gid
        var gid: VertexId = row._1

        // 12-3、取到今日的 ids。 tp._2为【id-->gid】,所以需要去重获得ids
        val ids: Set[VertexId] = row._2.toMap.keySet

        // 12-4、取到历史的 。从广播变量中 【id-->gid】
        val idmpLastDay: collection.Map[VertexId, VertexId] = bc.value
        val historyIdSet: collection.Set[VertexId] = idmpLastDay.keySet

        // 12-5、取历史idset和今日这一组idset的交集 intersect
        val sharedIds: Set[VertexId] = ids.intersect(historyIdSet)

        // 12-6、如果存在交集，则取历史的gid作为当日这组ids的gid
        if (sharedIds != null && sharedIds.nonEmpty) {
          gid = idmpLastDay(sharedIds.head)
        }

        // 12-7、再返回今日这一组结果 (id, gid)
        ids.map(id => (id, gid))
      })
    ajustedResult

    // 13、输出保存最终结果
    FileUtils.deleteDir(new File("user_profile/data/output/idmp/day01"))
    ajustedResult.toDF("id", "gid").coalesce(1).write.parquet("user_profile/data/output/idmp/day01")
    ajustedResult.toDF("id", "gid").show(100, false)

    // 14、关闭spark

    /** *
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
