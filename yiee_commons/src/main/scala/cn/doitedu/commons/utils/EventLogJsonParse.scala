package cn.doitedu.commons.utils

import java.lang

import cn.doitedu.commons.beans.EventLogBean
import com.alibaba.fastjson.{JSON, JSONObject}

import scala.io.{BufferedSource, Source}

/**
  * @author: 余辉  
  * @blog: https://blog.csdn.net/silentwolfyh
  * @create: 2019-11-08 10:27
  * @description:
  * 1、JSONObject的使用工具
  * 2、https://mvnrepository.com/artifact/com.alibaba/fastjson
  **/
object EventLogJsonParse {

  def main(args: Array[String]): Unit = {

    // 读取 eventLogJson 数据
    val json: String = Source.fromFile("user_profile/data/eventLogJson.txt").mkString

    /** *
      * fastjson使用
      *
      * JSON.parseObject 解析json数据，变成对象
      * jSONObject.getString  获取key值将value变成字符串
      * jSONObject.getLong    获取key值将value变成Long值
      * jSONObject.getJSONObject  获取key值将value变成Json对象
      */
    val jSONObject: JSONObject = JSON.parseObject(json)
    val logtype: String = jSONObject.getString("logType")
    val commit_time: lang.Long = jSONObject.getLong("commit_time")
    val event: JSONObject = jSONObject.getJSONObject("event")
    val u: JSONObject = jSONObject.getJSONObject("u")

    println(logtype)
    println(commit_time)
    println(event)
    println(u)

    /**
      * event 需要转为Map集合
      * scala.collection.JavaConversions  scala--java集合类型转换
      * event.getInnerMap.map(tp => (tp._1, tp._2.toString)).toMap
      */

    import scala.collection.JavaConversions._
    val eventMap: Map[String, String] = event.getInnerMap.map(tp => (tp._1, tp._2.toString)).toMap
    println("eventMap==>" + eventMap)
    val birthday: String = u.getString("birthday")
    println(birthday)
  }
}
