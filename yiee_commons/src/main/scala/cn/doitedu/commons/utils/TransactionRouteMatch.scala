package cn.doitedu.commons.utils

import scala.collection.mutable.ListBuffer

/**
  * @date: 2019/9/6
  * @site: www.doitedu.cn
  * @author: hunter.d 涛哥
  * @qq: 657270652
  * @description: 用户行为与业务路径的匹配工具
  */
object TransactionRouteMatch {

  /**
    *
    * @param userActions 用户行为事件序列
    * @param transSteps  业务定义的路径序列
    * @return 所满足的步骤列表
    */
  def routeMatch(userActions: List[String], transSteps: List[String]): List[Int] = {

    //  userActions :   A   B   B   D   A    C    F
    //  transSteps:     A   B   C   D
    //  返回：  List[1,2,3]

    val res = new ListBuffer[Int]

    var index = -1  // 每次搜索的起始位置
    var flag = true  // 退出循环的标志

    for (i <- 0 until transSteps.size if flag) {
      // 从业务步骤序列中取一个值，取用户行为序列中index+1的位置去搜索
      index = userActions.indexOf(transSteps(i), index+1)
      if (index != -1) {
        // 如果搜索到，则添加一个完成步骤到结果列表中
        res.+=(i+1)
      }else{
        // 如果搜索不到，则跳出循环不再搜索
        flag = false
      }
    }
    res.toList
  }


  /**
    * 测试代码
    * @param args
    */
  def main(args: Array[String]): Unit = {

    val u = List("C","D","B","D","A","C","F")
    val t = List("A","B","C","D")

    println(routeMatch(u, t))
  }

}
