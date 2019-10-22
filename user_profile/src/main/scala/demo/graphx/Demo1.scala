package demo.graphx

import java.io.File

import cn.doitedu.commons.utils.SparkUtil
import org.apache.commons.lang3.{StringEscapeUtils, StringUtils}
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row}
import cn.doitedu.commons.utils.FileUtils

import scala.collection.immutable

/**
 * @author: 余辉
 * @blog:   https://blog.csdn.net/silentwolfyh
 * @create: 2019/10/22
 * @description: 图计算入门demo
 **/

object Demo1 {

  def main(args: Array[String]): Unit = {

    // 1、建立session连接 import spark.implicits._
    val spark = SparkUtil.getSparkSession(this.getClass.getSimpleName)
    import spark.implicits._

    // 2、读取数据以及缓存起来
    val df = spark.read.option("header", true).csv("user_profile/demodata/graphx/input/demo1.dat")
    df.cache()
    // 3、将数据描述成点 vertcies,集合(RDD[Vertex]),字段为 phone，name，wx，返回array
    val vertcies: RDD[(Long, String)] = df.rdd.flatMap(row => {

      val phone = row.getAs[String]("phone")
      val name = row.getAs[String]("name")
      val wx = row.getAs[String]("wx")

      //3-1 过滤掉这一行中的空标识
      //3-2 将一个字符串标识，转换成一个Vertex,其实就是个tuple：(顶点hashCode,顶点id)
      Array(phone, name, wx).filter(str => StringUtils.isNoneBlank(str)).map(id => (id.hashCode.toLong, id))
    })

    vertcies.take(30).foreach(println)
    //(208397334,13866778899)(20977295,刘德华)(113568560,wx_hz)
    // (-1485777898,13877669988)(681286,华仔)(113568560,wx_hz)
    // (20977295,刘德华)(-774338670,wx_ldh)
    // (-1095633001,13912344321)(38771171,马德华)
    // (-774337709,wx_mdh)(-1095633001,13912344321)(20090824,二师兄)
    // (113568358,wx_bj)(-1007898506,13912664321)(29003441,猪八戒)  (113568358,wx_bj)


    // 4、描述点和点之间的边edges,val edges: RDD[Edge[String]]
    val edges: RDD[Edge[String]] = df.rdd.flatMap(row => {

      val phone = row.getAs[String]("phone")
      val name = row.getAs[String]("name")
      val wx = row.getAs[String]("wx")

      val ids = Array(phone, name, wx).filter(StringUtils.isNotBlank(_))
      //Edge （边）:A single directed edge consisting of a source id, target id, and the data associated with the edge.
      for (i <- 1 until ids.size) yield Edge(ids(0).hashCode.toLong, ids(i).hashCode.toLong, "")
    })

    // 5、用点集合和边集合，构造图数据模型Graph
    val graph = Graph(vertcies, edges)

    // 6、在图数据模型上调一个算法：求最大连通子图(Graph)
    val value: Graph[VertexId, String] = graph.connectedComponents()
    val connected = value.vertices
    connected.take(20).foreach(println)
    //(-1095633001,-1095633001)
    // (29003441,-1095633001)
    // (113568560,-1485777898)
    // (113568358,-1095633001)
    // 7、整理结果
    // gid -> 标识1,标识2,标识3,.....
    // gid -> 标识1,标识2,标识3,.....

    val resDF = vertcies.join(connected) // (38771171,("马德华",-1095633001))
      .map(tp => (tp._2._2, tp._2._1))
      .toDF("gid", "id")
    resDF.createTempView("res")

    FileUtils.deleteDir(new File("user_profile/demodata/graphx/out_idmp"))
    resDF.coalesce(1).distinct().write.parquet("user_profile/demodata/graphx/out_idmp")
    sys.exit(1) // TODO

    //8、查询 结果为 idmapping  ： select gid,collect_set(id) as ids from res group by gid
    val idmapping = spark.sql(
      """
        |
        |select
        |gid,
        |collect_set(id) as ids
        |
        |from res
        |group by gid
        |
         """.stripMargin)
    idmapping.show(10, false)

    /**
      * +-----------+--------------------------------------------------------+
      * |gid        |ids                                                     |
      * +-----------+--------------------------------------------------------+
      * |-1485777898|[华仔, 13866778899, 13877669988, wx_ldh, 刘德华, wx_hz]      |
      * |-1095633001|[wx_mdh, 13912664321, 猪八戒, wx_bj, 二师兄, 马德华, 13912344321]|
      * +-----------+--------------------------------------------------------+
      */

    FileUtils.deleteDir(new File("user_profile/demodata/graphx/out_idmp"))
    idmapping.write.parquet("user_profile/demodata/graphx/out_idmp")

    spark.close()
  }
}
