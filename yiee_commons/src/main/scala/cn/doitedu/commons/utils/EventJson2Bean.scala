package cn.doitedu.commons.utils

import cn.doitedu.commons.beans.EventLogBean
import com.alibaba.fastjson.JSON

/**
  * @date: 2019/8/27
  * @site: www.doitedu.cn
  * @author: hunter.d 涛哥
  * @qq: 657270652
  * @description: 事件日志json转成EventLogBean
  *
  *
  */
object EventJson2Bean {

  def genBean(json: String): EventLogBean = {

    try {
      // JSONObject是个什么？
      val jSONObject = JSON.parseObject(json)
      val logtype = jSONObject.getString("logType")
      val commit_time = jSONObject.getLong("commit_time")
      val event = jSONObject.getJSONObject("event")

      import scala.collection.JavaConversions._
      val eventMap = event.getInnerMap.map(tp => (tp._1, tp._2.toString)).toMap


      val u = jSONObject.getJSONObject("u")
      val cookieid = u.getString("cookieid")
      val account = u.getString("account")
      val sessionId = u.getString("sessionId")

      val phone = u.getJSONObject("phone")
      val imei = phone.getString("imei")
      val osName = phone.getString("osName")
      val osVer = phone.getString("osVer")
      val resolution = phone.getString("resolution")
      val androidId = phone.getString("androidId")
      val manufacture = phone.getString("manufacture")
      val deviceId = phone.getString("deviceId")


      val app = u.getJSONObject("app")
      val appid = app.getString("appid")
      val appVer = app.getString("appVer")
      val release_ch = app.getString("release_ch")
      val promotion_ch = app.getString("promotion_ch")


      val loc = u.getJSONObject("loc")
      val lng = loc.getDouble("longtitude")
      val areacode = loc.getLong("areacode")

      val lat = loc.getDouble("latitude")
      val carrier = loc.getString("carrier")
      val netType = loc.getString("netType")


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
