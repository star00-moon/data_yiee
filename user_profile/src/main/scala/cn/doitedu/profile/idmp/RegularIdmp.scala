package cn.doitedu.profile.idmp

import java.io.File

import cn.doitedu.commons.utils.{FileUtils, IdsExtractor, SparkUtil}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

/**
 * @author: 余辉
 * @blog:   https://blog.csdn.net/silentwolfyh
 * @create: 2019/10/22
 * @description: 常规的id映射字典计算程序
 **/
object RegularIdmp {

  def main(args: Array[String]): Unit = {
    //1、建立session连接
    Logger.getLogger("org").setLevel(Level.WARN)
    val spark = SparkUtil.getSparkSession(this.getClass.getSimpleName)
    import spark.implicits._

    //2、定义处理数据路径 cmccLogPath dspLogPath eventLogPath
    /*val cmccLogPath = "user_profile/demodata/idmp/input/day02/cmcclog"
    val dspLogPath = "user_profile/demodata/idmp/input/day02/dsplog"
    val eventLogPath = "user_profile/demodata/idmp/input/day02/eventlog"*/
    val cmccLogPath = "user_profile/data/cmcclog/day01"
    val dspLogPath = "user_profile/data/dsplog/day01"
    val eventLogPath = "user_profile/data/eventlog/day01"

    // 3、抽取当日的各类数据的id标识数据
    /*val cmccIds = IdsExtractor.extractDemoCmccLogIds(spark,cmccLogPath)
    val dspIds = IdsExtractor.extractDemoDspLogIds(spark,dspLogPath)
    val eventIds = IdsExtractor.extractDemoEventLogIds(spark,eventLogPath)*/
    val cmccIds = IdsExtractor.extractCmccLogIds(spark, cmccLogPath)
    val dspIds = IdsExtractor.extractDspLogIds(spark, dspLogPath)
    val eventIds = IdsExtractor.extractEventLogIds(spark, eventLogPath)

    // 4、查询cmccIds、dspIds、eventIds的并集
    // union是返回两个数据集的并集，不包括重复行，要求列数要一样，类型可以不同
    val ids = cmccIds.union(dspIds).union(eventIds)

    // 5、映射点集合
    val vertices: RDD[(Long, String)] = ids.flatMap(arr => {
      arr.map(strId => (strId.hashCode.toLong, strId))
    })

    // 6、 映射边集合
    val edges: RDD[Edge[String]] = ids.flatMap(arr => {
      val sortedArr = arr.sorted
      for (i <- 0 until sortedArr.size - 1; j <- i + 1 until sortedArr.size) yield {
        Edge(sortedArr(i).hashCode.toLong, sortedArr(j).hashCode.toLong, "")
      }
    })

    // 7、过滤出现次数<阈值的边
    val filteredEdges = edges.map(edge => (edge, 1)).reduceByKey(_ + _).filter(_._2 > 0).map(_._1)

    //  8、加载上日的idmapping数据
    val idmpDF = spark.read.parquet("user_profile/demodata/idmp/output/day01")

    // 8、将上日的idmp数据也映射成点集合和边集合
    val preIds = idmpDF.rdd.flatMap({
      case Row(id: Long, gid: Long) => Array((id, ""), (gid, ""))
    })
    val preEges = idmpDF.rdd.map({
      case Row(id: Long, gid: Long) => Edge(id, gid, "")
    })

    // 9、历史点集合+今日点集合， 历史边集合+今日边集合 ==》 构造图
    val graph = Graph(vertices.union(preIds), filteredEdges.union(preEges))

    // 10.求最大连通子图    id_x->gid
    val currentDayResult = graph.connectedComponents().vertices

    // 利用上日的IDmapping来调整今日的计算结果（保持gid的延续性）  先将历史idmapping数据收集到driver端为hashmap结构，并广播
    val idmpMap = idmpDF.rdd.map({
      case Row(id: Long, gid: Long) => (id, gid)
    }).collectAsMap()
    val bc = spark.sparkContext.broadcast(idmpMap)

    // 11、利用上日idmap调整当日计算结果
    val ajustedResult: RDD[(VertexId, VertexId)] = currentDayResult
      // 11-1、按今日gid分组
      .groupBy(_._2)
      .flatMap(tp => {
        // 11-2、取到一组ids的今日计算结果gid
        var gid = tp._1

        // 11-3、取到一组ids
        val idSet: Set[VertexId] = tp._2.toMap.keySet

        // 11-4、从广播变量中取所有历史id
        val idmpLastDay = bc.value
        val historyIdSet = idmpLastDay.keySet

        // 11-5、取历史idset和今日这一组idset的交集
        val sharedIds = idSet.intersect(historyIdSet)

        //  11-6、如果存在交集
        if (sharedIds != null && sharedIds.size > 0) {
          //  11-7、取历史的gid作为当日这组ids的gid
          gid = idmpLastDay.get(sharedIds.head).get
        }

        // 11-8、再返回这一组结果
        idSet.map(id => (id, gid))

      })

    // 11-9、输出保存最终结果
    FileUtils.deleteDir(new File("user_profile/data/output/idmp/day01"))
    ajustedResult.toDF("id", "gid").coalesce(1).write.parquet("user_profile/data/output/idmp/day01")

    ajustedResult.toDF("id", "gid").show(100, false)

    spark.close()
  }

}
