package cn.doitedu.sparkml.kmeans

import cn.doitedu.commons.utils.SparkUtil
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable

/**
  * @author: 余辉
  * @blog: https://blog.csdn.net/silentwolfyh
  * @create: 2019/10/21
  * @description:
  * 1、利用kmeans做人群聚类分析  示例程序
  **/
object KMeansDemo {

  def main(args: Array[String]): Unit = {

    // 1、建立session连接
    val spark: SparkSession = SparkUtil.getSparkSession(this.getClass.getSimpleName)
    val df: DataFrame = spark.read.option("inferSchema", true).option("header", true).csv("rec_system/data/kmeans/a.txt")
    import spark.implicits._

    /** *
      * 2、字段处理变成数字
      * id,queke,chidao,score,zaozixi,wanzixi,keshui
      * StringIndexer类 ：可以把一些只有若干个取值的文字型特征变成数字
      */
    // 2-1、处理早自习字段
    val stringindexer1: StringIndexer = new StringIndexer()
      // 2-1-1、选择输入字段
      .setInputCol("wanzixi")
      // 2-1-2、选择输出字段
      .setOutputCol("wzx")
    // 2-1-3、先训练fit 再转换 transform
    val df1: DataFrame = stringindexer1.fit(df).transform(df)

    // 2-2、处理晚自习字段
    val stringindexer2: StringIndexer = new StringIndexer()
      // 2-2-1、选择输入字段
      .setInputCol("zaozixi")
      // 2-2-2、选择输出字段
      .setOutputCol("zzx")
    // 2-2-3、先训练fit 再转换 transform，且删除【zaozixi】和【wanzixi】列
    val df2: DataFrame = stringindexer2.fit(df1).transform(df1).drop("zaozixi").drop("wanzixi")

    // 3、注册UDF函数，将数组转为向量  Vectors.dense(arr.toArray)
    import org.apache.spark.sql.functions._
    val tovec: UserDefinedFunction = udf((arr: mutable.WrappedArray[Double]) => {
      Vectors.dense(arr.toArray)
    })

    // 4、将特征值组成向量
    val vec: DataFrame = df2.select('id, tovec(array('queke, 'chidao, 'score, 'keshui, 'wzx, 'zzx)).as("f"))

    // 5、kmeans 训练模型
    val kmeans: KMeans = new KMeans()
      .setK(3)
      .setFeaturesCol("f")
      .setSeed(10)

    // 6、先训练fit 再转换 transform
    val res: DataFrame = kmeans.fit(vec).transform(vec)
    res.show(100, false)

    // 7、spark关闭
    spark.close()
  }
}
