package cn.doitedu.course.dw.route

import java.util.Properties

import cn.doitedu.commons.utils.{SparkUtil, TransactionRouteMatch}
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ListBuffer

/**
  * @date: 2019/9/6
  * @site: www.doitedu.cn
  * @author: hunter.d 涛哥
  * @qq: 657270652
  * @description: 业务路径转化漏斗分析
  */
object LoudouAnalysis {

  def main(args: Array[String]): Unit = {

    val spark = SparkUtil.getSparkSession()
    import spark.implicits._

    // 加载业务路径定义 元数据
    val props = new Properties()
    props.setProperty("user", "root")
    props.setProperty("password", "root")
    val routeDefine = spark.read.jdbc("jdbc:mysql://localhost:3306/yieemeta", "transaction_route", props)

    routeDefine.show(20, false)

    // 加载用户访问路径数据
    val userActionRoute = spark.read.option("header", "true").csv("data_ware/data/dws_user_acc_route/part-00000.csv")
    userActionRoute.show(20, false)


    /**
      * 接下来要做的事，就是去判断每个人的行为，是否满足某个业务的某个步骤定义，如果满足，则输出：
      * u_id,t_id,t_step
      * 怎么做呢？怎么做都好说，问题在于，判断标准的是什么？
      * 对于一个人的行为，是否满足某业务的步骤定义，可能有如下界定标准：
      * 比如，业务定义的步骤事件分别为： A B C D
      * 假如，某个人的行为记录为：
      * 张三： A  A  B  A  B  C
      * 李四： C  D  A  B  C  E  D
      * 王五： A  B  B  C  E  A  D
      * 赵六： B  C  E  E  D
      * 那么：这些算满足了业务定义的哪些步骤？
      * 标准1：  判断是否满足业务C步骤，必须要求前面两个近邻的事件是A B
      * 标准2：  判断是否满足业务C步骤，只要求C事件发生前，前面发生过 B ，B前面发生过A，不要求紧邻
      * 标准3：  判断是否满足业务C步骤，只要发生了C事件且前面发生B就行
      * 标准4：  判断是否满足业务C步骤，只要发生了C事件就算
      *
      * 那么：在写代码时，究竟按哪个标准来计算？ 看公司开会讨论的需求！
      * 咱们下面以 标准2 为例！
      *
      * 做法：
      * 将一个用户的所有行为按时间先后顺序收集到一起：A  A  B  A  B  C
      * 将业务路径定义做成广播变量：
      * Map(
      * "t101" -> list(A,B,C,D)
      * "t102" -> list(D,B,C)
      * )
      * 然后，将这个人的行为和业务定义去对比，按标准2对比
      * 具体来说：
      * 拿业务定义中的步骤1，去用户的行为序列中搜索，如果搜索到，则继续
      * 拿步骤2，去用户行为序列种步骤1事件后面去搜索，如果搜索到，则继续
      * 以此类推!
      *
      *
      **/


    // 将业务路径定义变成hashmap格式
    /**
      * Map(
      * "t101" -> list[(1,A),(2,B),(3,C),(4,D)]
      * "t102" -> list[(1,D),(2,B),(3,C)]
      * )
      */
    val routeMap: collection.Map[String, List[(Int, String)]] = routeDefine
      .rdd
      .map(row => {

        val t_id = row.getAs[String]("tid")
        val t_event = row.getAs[String]("event_id")
        val t_step = row.getAs[Int]("step")

        (t_id, (t_step, t_event))
      })
      .groupByKey()
      .mapValues(iter => iter.toList.sortBy(_._1))
      .collectAsMap()

    // 广播
    val bc = spark.sparkContext.broadcast(routeMap)


    // 处理用户行为
    val x: RDD[((String, String), String)] = userActionRoute
      .select("uid", "sid", "step", "url")
      .rdd
      .map(row => {
        val uid = row.getAs[String]("uid")
        val sid = row.getAs[String]("sid")
        val step = row.getAs[String]("step")
        val url = row.getAs[String]("url")
        (uid, sid, step.toInt, url)
      })
      .groupBy(tp => (tp._1, tp._2)) // 按uid和sid分组
      .mapValues(iter => {
      // 这里就是一个人的一次会话中所有行为事件，并且按次序排好序了
      val actList = iter.toList.sortBy(tp => tp._3)
      // 将这个人的这些事件中的url拿来拼一个字符串
      val actStr = actList.map(_._4).mkString("")

      actStr
    })

    x.take(20).foreach(println)
    //((u01,s01),XYABCBO)
    //((u02,s02),ACABDBC)


    // 拿处理好的用户行为记录，去比对业务路径，看满足哪些业务的哪些步骤

    // 业务字典：
    /**
      * bc:
      * Map(
      * "t101" -> list[(1,A),(2,B),(3,C),(4,D)]
      * "t102" -> list[(1,D),(2,B),(3,C)]
      * )
      */

    // 用户行为 x
    //((u01,s01),XYABCBO)
    //((u02,s02),ACABDBC)


    // TODO  套算法
    val res = x
      .flatMap(tp => {
        val uid = tp._1._1
        val userActions: List[String] = tp._2.toList.map(c => c.toString) // 用户的行为事件序列
        val transRoutes: collection.Map[String, List[(Int, String)]] = bc.value

        // 构造一个listbuff来装结果（uid,t_id,t_step）
        val resList = new ListBuffer[(String, String, Int)]

        // 遍历每一项业务
        for ((k, v) <- transRoutes) {

          val transSteps = v.map(_._2) // 业务中的步骤事件序列

          // 调算法
          val okSteps: List[Int] = TransactionRouteMatch.routeMatch(userActions, transSteps)
          val resOneTrans = okSteps.map(okStep => (uid, k, okStep))
          resList ++= resOneTrans

        }
        resList
      })
      .toDF("uid", "tid", "stepid")

    res.show(20, false)

    spark.close()

  }

}
