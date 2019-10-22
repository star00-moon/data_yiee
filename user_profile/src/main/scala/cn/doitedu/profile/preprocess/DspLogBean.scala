package cn.doitedu.profile.preprocess

case class DspLogBean(
                       var sessionid: String, // 会话标识
                       var advertisersid: Int, // 广告主id
                       var adorderid: Int, // 广告id
                       var adcreativeid: Int, // 广告创意id ( >= 200000 : dsp)
                       var adplatformproviderid: Int, // 广告平台商id (>= 100000: rtb)
                       var sdkversion: String, // sdk 版本号
                       var adplatformkey: String, // 平台商key
                       var putinmodeltype: Int, // 针对广告主的投放模式, //1：展示量投放 2：点击
                       var requestmode: Int, // 数据请求方式（1:请求、2:展示、3:点击）
                       var adprice: Double, // 广告价格
                       var adppprice: Double, // 平台商价格
                       var requestdate: String, // 请求时间, //格式为：yyyy-m-dd hh:mm:ss
                       var ip: String, // 设备用户的真实ip 地址
                       var appid: String, // 应用id
                       var appname: String, // 应用名称
                       var uuid: String, // 设备唯一标识
                       var device: String, // 设备型号，如htc、iphone
                       var client: Int, // 操作系统（1：android 2：ios 3：wp）
                       var osversion: String, // 设备操作系统版本
                       var density: String, // 设备屏幕的密度
                       var pw: Int, // 设备屏幕宽度
                       var ph: Int, // 设备屏幕高度
                       var lng: Double, // 设备所在经度
                       var lat: Double, // 设备所在纬度
                       var provincename: String, // 设备所在省份名称
                       var cityname: String, // 设备所在城市名称
                       var ispid: Int, // 运营商id
                       var ispname: String, // 运营商名称
                       var networkmannerid: Int, // 联网方式id
                       var networkmannername: String, //联网方式名称
                       var iseffective: Int, // 有效标识（有效指可以正常计费的）(0：无效1：
                       var isbilling: Int, // 是否收费（0：未收费1：已收费）
                       var adspacetype: Int, // 广告位类型（1：banner 2：插屏3：全屏）
                       var adspacetypename: String, // 广告位类型名称（banner、插屏、全屏）
                       var devicetype: Int, // 设备类型（1：手机2：平板）
                       var processnode: Int, // 流程节点（1：请求量kpi 2：有效请求3 ：广告请求）
                       var apptype: Int, // 应用类型id
                       var district: String, // 设备所在县名称
                       var paymode: Int, // 针对平台商的支付模式，1：展示量投放(CPM) 2：点击
                       var isbid: Int, // 是否rtb
                       var bidprice: Double, // rtb 竞价价格
                       var winprice: Double, // rtb 竞价成功价格
                       var iswin: Int, // 是否竞价成功
                       var cur: String, // values:usd|rmb 等
                       var rate: Double, // 汇率
                       var cnywinprice: Double, // rtb 竞价成功转换成人民币的价格
                       var imei: String, // imei
                       var mac: String, // mac
                       var idfa: String, // idfa
                       var openudid: String, // openudid
                       var androidid: String, // androidid
                       var rtbprovince: String, // rtb 省
                       var rtbcity: String, // rtb 市
                       var rtbdistrict: String, // rtb 区
                       var rtbstreet: String, // rtb 街道
                       var storeurl: String, // app 的市场下载地址
                       var realip: String, // 真实ip
                       var isqualityapp: Int, //优选标识
                       var bidfloor: Double, //底价
                       var aw: Int, // 广告位的宽
                       var ah: Int, // 广告位的高
                       var imeimd5: String, //imei_md5
                       var macmd5: String, //mac_md5
                       var idfamd5: String, //idfa_md5
                       var openudidmd5: String, // openudid_md5
                       var androididmd5: String, // androidid_md5
                       var imeisha1: String, // imei_sha1
                       var macsha1: String, // mac_sha1
                       var idfasha1: String, // idfa_sha1
                       var openudidsha1: String, // openudid_sha1
                       var androididsha1: String, // androidid_sha1
                       var uuidunknow: String, // uuid_unknow tanx 密文
                       var userid: String, // 平台用户id
                       var iptype: Int, // 表示ip 类型
                       var initbidprice: Double, // 初始出价
                       var adpayment: Double, // 转换后的广告消费
                       var agentrate: Double, // 代理商利润率
                       var lrate: Double, // 代理利润率
                       var adxrate: Double, // 媒介利润率
                       var title: String, // 标题
                       var keywords: String, // 关键字
                       var tagid: String, // 广告位标识(当视频流量时值为视频ID 号)
                       var callbackdate: String, // 回调时间格式为:YYYY/mm/dd hh:mm:ss
                       var channelid: String, // 频道ID
                       var mediatype: Int, // 媒体类型：1 长尾媒体2 视频媒体3 独立媒体默认:1
                       var biz: String, // 追加字段
                       var appDesc: String, // 追加字段
                       var gid: String // 追加字段
                     )

/***
  * 字符串变成 Int，Double，Long的转换方法
  */
class MyString(s: String) {
  def toIntPlus: Int = {
    var x: Int = 0
    try {
      x = s.toInt
    } catch {
      case e: Exception =>
    }
    x
  }

  def toDoublePlus: Double = {
    var x: Double = 0.0
    try {
      x = s.toDouble
    } catch {
      case e: Exception =>
    }
    x
  }

  def toLongPlus: Long = {
    var x: Long = 0L
    try {
      x = s.toLong
    } catch {
      case e: Exception =>
    }
    x
  }
}


object DspLogBean {

  //隐士转换
  implicit def str2MyStr(s: String): MyString = new MyString(s)

  def genDspLogBean(arr: Array[String]): DspLogBean = {
    DspLogBean(
      arr(0),
      arr(1).toIntPlus,
      arr(2).toIntPlus,
      arr(3).toIntPlus,
      arr(4).toIntPlus,
      arr(5),
      arr(6),
      arr(7).toIntPlus,
      arr(8).toIntPlus,
      arr(9).toDoublePlus,
      arr(10).toDoublePlus,
      arr(11),
      arr(12),
      arr(13),
      arr(14),
      arr(15),
      arr(16),
      arr(17).toIntPlus,
      arr(18),
      arr(19),
      arr(20).toIntPlus,
      arr(21).toIntPlus,
      arr(22).toDoublePlus,
      arr(23).toDoublePlus,
      arr(24),
      arr(25),
      arr(26).toIntPlus,
      arr(27),
      arr(28).toIntPlus,
      arr(29),
      arr(30).toIntPlus,
      arr(31).toIntPlus,
      arr(32).toIntPlus,
      arr(33),
      arr(34).toIntPlus,
      arr(35).toIntPlus,
      arr(36).toIntPlus,
      arr(37),
      arr(38).toIntPlus,
      arr(39).toIntPlus,
      arr(40).toDoublePlus,
      arr(41).toDoublePlus,
      arr(42).toIntPlus,
      arr(43),
      arr(44).toDoublePlus,
      arr(45).toDoublePlus,
      arr(46),
      arr(47),
      arr(48),
      arr(49),
      arr(50),
      arr(51),
      arr(52),
      arr(53),
      arr(54),
      arr(55),
      arr(56),
      arr(57).toIntPlus,
      arr(58).toDoublePlus,
      arr(59).toIntPlus,
      arr(60).toIntPlus,
      arr(61),
      arr(62),
      arr(63),
      arr(64),
      arr(65),
      arr(66),
      arr(67),
      arr(68),
      arr(69),
      arr(70),
      arr(71),
      arr(72),
      arr(73).toIntPlus,
      arr(74).toDoublePlus,
      arr(75).toDoublePlus,
      arr(76).toDoublePlus,
      arr(77).toDoublePlus,
      arr(78).toDoublePlus,
      arr(79),
      arr(80),
      arr(81),
      arr(82),
      arr(83),
      arr(84).toIntPlus,
      "",
      "",
      ""
    )
  }
}