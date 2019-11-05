package cn.doitedu.commons.utils

import java.lang

import cn.doitedu.commons.beans.EventLogBean
import com.alibaba.fastjson.{JSON, JSONObject}

/**
 * @author: 余辉
 * @blog:   https://blog.csdn.net/silentwolfyh
 * @create: 2019/10/22
 * @description: 事件日志json转成EventLogBean
 **/
object EventJson2Bean {

  def genBean(json: String): EventLogBean = {

    try {
      // JSONObject是个什么？
      val jSONObject: JSONObject = JSON.parseObject(json)
      val logtype: String = jSONObject.getString("logType")
      val commit_time: lang.Long = jSONObject.getLong("commit_time")
      val event: JSONObject = jSONObject.getJSONObject("event")

      import scala.collection.JavaConversions._
      val eventMap: Map[String, String] = event.getInnerMap.map(tp => (tp._1, tp._2.toString)).toMap

      val u: JSONObject = jSONObject.getJSONObject("u")
      val cookieid: String = u.getString("cookieid")
      val account: String = u.getString("account")
      val sessionId: String = u.getString("sessionId")

      val phone: JSONObject = u.getJSONObject("phone")
      val imei: String = phone.getString("imei")
      val osName: String = phone.getString("osName")
      val osVer: String = phone.getString("osVer")
      val resolution: String = phone.getString("resolution")
      val androidId: String = phone.getString("androidId")
      val manufacture: String = phone.getString("manufacture")
      val deviceId: String = phone.getString("deviceId")

      val app: JSONObject = u.getJSONObject("app")
      val appid: String = app.getString("appid")
      val appVer: String = app.getString("appVer")
      val release_ch: String = app.getString("release_ch")
      val promotion_ch: String = app.getString("promotion_ch")

      val loc: JSONObject = u.getJSONObject("loc")
      val lng: lang.Double = loc.getDouble("longtitude")
      val areacode: lang.Long = loc.getLong("areacode")

      val lat: lang.Double = loc.getDouble("latitude")
      val carrier: String = loc.getString("carrier")
      val netType: String = loc.getString("netType")


      EventLogBean(
        cookieid,
        account,
        imei,
        osName,
        osVer,
        resolution,
        androidId,
        manufacture,
        deviceId,
        appid,
        appVer,
        release_ch,
        promotion_ch,
        areacode,
        lng,
        lat,
        carrier,
        netType,
        sessionId,
        logtype,
        commit_time,
        eventMap
      )
    } catch {
      case _:Throwable => null
    }
  }

}
