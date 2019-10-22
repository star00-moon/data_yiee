package cn.doitedu.sparkml.kmeans

import cn.doitedu.commons.utils.SparkUtil
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.DataFrame

import scala.collection.mutable

/**
  * @author: 余辉
  * @blog: https://blog.csdn.net/silentwolfyh
  * @create: 2019/10/21
  * @description: 利用kmeans做人群聚类分析  示例程序
  **/
object KMeansDemo {

  def main(args: Array[String]): Unit = {
    // id,queke,chidao,score,zaozixi,wanzixi,keshui
    val spark = SparkUtil.getSparkSession(this.getClass.getSimpleName)
    val df = spark.read.option("inferSchema", true).option("header", true).csv("rec_system/data/kmeans/a.txt")
    import spark.implicits._

    // 可以把一些只有若干个取值的文字型特征变成数字
    val stringindexer1 = new StringIndexer()
      .setInputCol("wanzixi")
      .setOutputCol("wzx")

    val df1 = stringindexer1.fit(df).transform(df)

    val stringindexer2 = new StringIndexer()
      .setInputCol("zaozixi")
      .setOutputCol("zzx")

    // 文字符号标签特征数字化
    val df2 = stringindexer2.fit(df1).transform(df1).drop("zaozixi").drop("wanzixi")

    import org.apache.spark.sql.functions._
    val tovec = udf((arr: mutable.WrappedArray[Double]) => {
      Vectors.dense(arr.toArray)
    })

    // 将特征值组成向量
    val vec = df2.select('id, tovec(array('queke, 'chidao, 'score, 'keshui, 'wzx, 'zzx)).as("f"))

    // 训练模型
    val kmeans = new KMeans()
      .setK(3)
      .setFeaturesCol("f")
      .setSeed(10)

    val res: DataFrame = kmeans.fit(vec).transform(vec)
    res.show(100, false)
    spark.close()
  }
}
