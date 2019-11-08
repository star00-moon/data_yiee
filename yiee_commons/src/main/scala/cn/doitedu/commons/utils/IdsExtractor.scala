package cn.doitedu.commons.utils

import org.apache.commons.lang3.StringUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}

/**
  * @author: 余辉
  * @blog: https://blog.csdn.net/silentwolfyh
  * @create: 2019/10/22
  * @description:
  * 1、各类源数据(dsp,event,cmcc)的id标识抽取程序
  * 2、测试数据，通过map切分过滤空字段，转为 RDD[Array[String]]
  * 3、真实数据，dsp，获取 arr(46), arr(47), arr(48), arr(49), arr(50)
  * 4、真实数据，event，获取 b.imei, b.deviceId, b.androidId, b.account, b.cookieid
  * 5、真实数据，event，获取 arr(6), arr(7), arr(8)
  **/
object IdsExtractor {

  /**
    * 抽取dsp竞价日志demo数据
    *
    * @param spark
    * @param path
    * @return
    */
  def extractDemoDspLogIds(spark: SparkSession, path: String): RDD[Array[String]] = {
    val dspLog: Dataset[String] = spark.read.textFile(path)
    dspLog.rdd.map(line => {
      val arr: Array[String] = line.split(",")
      arr.filter(StringUtils.isNoneBlank(_))
    })
  }

  /**
    * 抽取公司内部事件日志demo数据
    *
    * @param spark
    * @param path
    * @return
    */
  def extractDemoEventLogIds(spark: SparkSession, path: String): RDD[Array[String]] = {
    val eventlog: Dataset[String] = spark.read.textFile(path)
    eventlog.rdd.map(line => {
      val arr: Array[String] = line.split(",")
      arr.filter(id => StringUtils.isNotBlank(id))
    })
  }

  /**
    * 抽取cmcc流量日志demo数据
    *
    * @param spark
    * @param path
    * @return
    */
  def extractDemoCmccLogIds(spark: SparkSession, path: String): RDD[Array[String]] = {
    val cmcclog: Dataset[String] = spark.read.textFile(path)
    cmcclog.rdd.map(line => {
      val arr: Array[String] = line.split(",")
      arr.filter(id => StringUtils.isNotBlank(id))
    })
  }

  /**
    * 抽取dsp竞价日志demo数据
    * 1、map切分之后，过滤空值，都统一成格式：Array(id1,id2,id3,....)
    * 2、需要的字段：【imei、mac、idfa、openudid、androidid】
    * 3、需要的字段：arr(46), arr(47), arr(48), arr(49), arr(50)
    *
    * @param spark
    * @param path
    * @return
    */
  def extractDspLogIds(spark: SparkSession, path: String): RDD[Array[String]] = {
    val dsplog: Dataset[String] = spark.read.textFile(path)
    dsplog.rdd.map(line => {
      val arr: Array[String] = line.split(",", -1)
      Array(arr(46), arr(47), arr(48), arr(49), arr(50)).filter(StringUtils.isNotBlank(_))
    })
  }

  /**
    * 抽取公司内部事件日志demo数据
    * 1、map切分之后，过滤空值，都统一成格式：Array(id1,id2,id3,....)
    * 2、需要的字段：【imei、deviceId、androidId、account、cookieid】
    *
    * @param spark
    * @param path
    * @return
    */
  def extractEventLogIds(spark: SparkSession, path: String): RDD[Array[String]] = {

    val eventlog: Dataset[String] = spark.read.textFile(path)

    eventlog.rdd.map(line => {
      line.split(" --> ", -1)
    }).filter(_.size > 1)
      .map(arr => {
        EventJson2Bean.genBean(arr(1))
      })
      .filter(b => b != null)
      .map(b => {
        Array(b.imei, b.deviceId, b.androidId, b.account, b.cookieid).filter(StringUtils.isNoneBlank(_))
      })

  }

  /**
    * 抽取cmcc流量日志数据
    * 1、map切分之后，过滤空值，都统一成格式：Array(id1,id2,id3,....)
    * 2、需要的字段：【arr(6)、arr(7)、arr(8)】
    *
    * @param spark
    * @param path
    * @return
    */
  def extractCmccLogIds(spark: SparkSession, path: String): RDD[Array[String]] = {
    val cmcclog: Dataset[String] = spark.read.textFile(path)
    cmcclog.rdd.map(line => {
      val arr: Array[String] = line.split("\t", -1)
      Array(arr(6), arr(7), arr(8)).filter(id => StringUtils.isNotBlank(id))
    })
  }

  def main(args: Array[String]): Unit = {
    val dspLogPath = "user_profile/demodata/idmp/input/day01/dsplog";
    val eventLogPath = "user_profile/demodata/idmp/input/day01/eventlog";
    val cmccLogPath = "user_profile/demodata/idmp/input/day01/cmcclog";

    val spark: SparkSession = SparkUtil.getSparkSession(this.getClass.getTypeName)
    val ids: RDD[Array[String]] = extractDemoDspLogIds(spark, dspLogPath)
    ids.flatMap(line => (line)).foreach(println)
  }
}
