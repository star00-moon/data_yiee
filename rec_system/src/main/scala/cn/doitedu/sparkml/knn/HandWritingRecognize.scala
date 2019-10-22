package cn.doitedu.sparkml.knn

import cn.doitedu.commons.utils.SparkUtil
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.linalg
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}


/**
  * @author: 余辉
  * @blog: https://blog.csdn.net/silentwolfyh
  * @create: 2019/10/21
  * @description: 手写数字识别
  **/
object HandWritingRecognize {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    val spark = SparkUtil.getSparkSession(this.getClass.getSimpleName)

    // 加载样本
    val sample = loadTraining(spark, "rec_system/demodata/digitTranningVec")
    sample.createTempView("sample")

    // 加载测试数据集
    val test = loadTest(spark, "rec_system/demodata/digitTestVec")
    test.createTempView("test")

    // 求欧式距离的udf
    val sqDist = (v1: linalg.Vector, v2: linalg.Vector) => {
      Vectors.sqdist(v1, v2)
    }
    spark.udf.register("sqdist", sqDist)

    val prediction = spark.sql(
      """
        |select
        |filename,
        |alabel,
        |blabel as prediction
        |from
        |(
        |select
        |filename,
        |alabel,
        |blabel,
        |row_number() over(partition by filename order by cnts desc ) as rn
        |from
        |(
        |   select
        |     filename,
        |     alabel,
        |     blabel,
        |     count(1) as cnts
        |   from
        |       (
        |          select
        |          filename,
        |          alabel,
        |          blabel,
        |          row_number() over(partition by filename order by dist) as rn
        |          from
        |          (
        |              select
        |              a.filename,
        |              a.label as alabel,
        |              b.label as blabel,
        |              sqdist(a.features,b.features) as dist
        |              from test a cross join sample b
        |          ) o1
        |
        |   ) o2
        |where o2.rn <=5
        |group by filename,alabel,blabel
        |) o3
        |) o4
        |where rn=1
      """.stripMargin)


    // |filename |alabel|prediction|
    //+---------+------+----------+
    //|3_78.txt |3.0   |3.0       |
    //|5_106.txt|5.0   |5.0       |
    //|5_23.txt |5.0   |5.0       |
    prediction.show(10, false)

    val rdd: RDD[(Double, Double)] = prediction
      .select("prediction", "alabel")
      .rdd
      .map(row => (row.getAs[Double]("prediction"), row.getAs[Double]("alabel")))


    // 计算准确率
    val metrics = new MulticlassMetrics(rdd)
    println("预测准确率为： " + metrics.accuracy)

    spark.close()
  }

  // 加载样本
  def loadTraining(spark: SparkSession, path: String): DataFrame = {
    import spark.implicits._

    val ds: Dataset[String] = spark.read.textFile(path)
    // 10.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,1.0,1.0,1.0,1.0,1.0,1.0,0.0,0.0,0.0,0.0,0.0,0.
    import spark.implicits._
    val df: DataFrame = ds.rdd.map(line => {
      val split = line.split("\001")
      val lable = split(0).toDouble
      val featuresArr = split(1).split(",").map(_.toDouble)
      val vec = Vectors.dense(featuresArr)
      (lable, vec)
    })
      .toDF("label", "features")
    df
  }

  /***
    * 加载测试数据集
    */
  def loadTest(spark: SparkSession, path: String): DataFrame = {
    import spark.implicits._

    val ds: Dataset[String] = spark.read.textFile(path)
    // 10.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,1.0,1.0,1.0,1.0,1.0,1.0,0.0,0.0,0.0,0.0,0.0,0.
    import spark.implicits._
    val df: DataFrame = ds.rdd.map(line => {
      val split = line.split("\001")
      val filename = split(0)
      val split2 = split(1).split("\002")
      val label = split2(0).toDouble

      val featuresArr = split2(1).split(",").map(_.toDouble)
      val vec = Vectors.dense(featuresArr)
      (filename, label, vec)
    })
      .toDF("filename", "label", "features")
    df
  }
}
