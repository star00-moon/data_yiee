package cn.doitedu.commons.beans

/**
  * 日志数据封装bean
  * @param cookieid
  * @param account
  * @param imei
  * @param osName
  * @param osVer
  * @param resolution
  * @param androidId
  * @param manufacture
  * @param deviceId
  * @param appid
  * @param appVer
  * @param release_ch
  * @param promotion_ch
  * @param areacode
  * @param longtitude
  * @param latitude
  * @param carrier
  * @param netType
  * @param sessionId
  * @param eventType
  * @param commit_time
  * @param event
  * @param province
  * @param city
  * @param district
  */
case class EventLogBean(
       cookieid: String,   // 用户的标识之一
       account: String,   // 用户的标识之一
       imei: String,  // 用户的标识之一
       osName: String,  // 操作系统名称
       osVer: String,  // 操作系统版本
       resolution: String,  // 手机屏幕分辨率
       androidId: String, // 安卓系统的软id  用户的标识之一
       manufacture: String,  // 手机生产厂商（品牌）
       deviceId: String,   // 设备id（用户标识之一）
       appid: String,   // 所用的app的标识
       appVer: String,   // 所用的app的版本
       release_ch: String,   // 所用的app的下载渠道（360应用市场，豌豆荚，小米应用....)
       promotion_ch: String,  // 所用的app的推广渠道（头条，百度，抖音.....)
       areacode: Long,   // 业务系统中用于标识一个地理位置的码（废弃）
       longtitude: Double,  // 本条日志发生的地点：经度
       latitude: Double,  // 本条日志发生的地点：维度
       carrier: String,   // 用户所用的移动运营商
       netType: String,  // 用户所用的网络类型（3g  4g  5g   wifi ）
       sessionId: String,  // 本条日志所在的会话id
       eventType: String,  // 本条日志所代表的用户操作事件类别
       commit_time: Long,   // 本条日志发生的时间
       event: Map[String, String],  // 本条日志所代表的事件的详细信息（所发生的页面，前一个页面，所点击的商品skuid...)
       var province: String = "",
       var city: String = "",
       var district: String = "",
       var gid:String = "-1",   // 全局统一标识id
       var title_kwds:String = ""  // 页面标题分词结果


  )
