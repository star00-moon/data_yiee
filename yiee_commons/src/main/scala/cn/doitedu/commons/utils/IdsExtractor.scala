package cn.doitedu.commons.utils

import cn.doitedu.commons.beans.EventLogBean
import org.apache.commons.lang3.StringUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * @date: 2019/9/15
  * @site: www.doitedu.cn
  * @author: hunter.d 涛哥
  * @qq: 657270652
  * @description: 各类源数据的id标识抽取程序
  */
object IdsExtractor {

  /**
    * 抽取公司内部事件日志demo数据
    *
    * @param spark
    * @param path
    * @return
    */
  def extractDemoEventLogIds(spark: SparkSession, path: String): RDD[Array[String]] = {

    val eventlog = spark.read.textFile(path)

    eventlog.rdd.map(line => {
      val arr = line.split(",")
      arr.filter(id => StringUtils.isNotBlank(id))
    })
  }

  /**
    * 抽取公司内部事件日志demo数据
    *
    * @param spark
    * @param path
    * @return
    */
  def extractEventLogIds(spark: SparkSession, path: String): RDD[Array[String]] = {

    val eventlog = spark.read.textFile(path)

    eventlog.rdd.map(line => {
      line.split(" --> ", -1)
    }).filter(_.size > 1)
      .map(arr => {
        EventJson2Bean.genBean(arr(1))
      })
      .filter(b => b != null)
      .map(b=>{
        Array(b.imei, b.deviceId, b.androidId, b.account, b.cookieid).filter(StringUtils.isNoneBlank(_))
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

    val cmcclog = spark.read.textFile(path)
    // 整理格式，将各种日志抽取的id字段，都统一成格式：Array(id1,id2,id3,....)
    cmcclog.rdd.map(line => {
      val arr = line.split(",")
      arr.filter(id => StringUtils.isNotBlank(id))
    })
  }

  /**
    * 抽取cmcc流量日志数据
    *
    * @param spark
    * @param path
    * @return
    */
  def extractCmccLogIds(spark: SparkSession, path: String): RDD[Array[String]] = {

    val cmcclog = spark.read.textFile(path)
    // 整理格式，将各种日志抽取的id字段，都统一成格式：Array(id1,id2,id3,....)
    cmcclog.rdd.map(line => {
      val arr = line.split("\t", -1)
      Array(arr(6), arr(7), arr(8)).filter(id => StringUtils.isNotBlank(id))
    })
  }


  /**
    * 抽取dsp竞价日志demo数据
    *
    * @param spark
    * @param path
    * @return
    */
  def extractDemoDspLogIds(spark: SparkSession, path: String): RDD[Array[String]] = {
    val dsplog = spark.read.textFile(path)
    dsplog.rdd.map(line => {
      val arr = line.split(",")
      arr.filter(id => StringUtils.isNotBlank(id))
    })
  }

  /**
    * 抽取dsp竞价日志demo数据
    *
    * @param spark
    * @param path
    * @return
    */
  def extractDspLogIds(spark: SparkSession, path: String): RDD[Array[String]] = {
    val dsplog = spark.read.textFile(path)
    dsplog.rdd.map(line => {
      val arr = line.split(",", -1)
      Array(arr(46), arr(47), arr(48), arr(49), arr(50)).filter(StringUtils.isNotBlank(_))
    })
  }
}
