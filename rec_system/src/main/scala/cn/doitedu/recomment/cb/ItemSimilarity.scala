package cn.doitedu.recomment.cb

import cn.doitedu.commons.utils.SparkUtil
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.feature.{HashingTF, IDF, StringIndexer, VectorSlicer}
import org.apache.spark.ml.linalg
import org.apache.spark.ml.linalg.{SparseVector, Vectors}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.DataTypes

import scala.collection.mutable

/**
  * @author: 余辉
  * @blog: https://blog.csdn.net/silentwolfyh
  * @create: 2019/10/21
  * @description: 物品相似度计算
  **/
object ItemSimilarity {
  def main(args: Array[String]): Unit = {

    // 1、建立session连接
    Logger.getLogger("org").setLevel(Level.WARN)
    val spark: SparkSession = SparkUtil.getSparkSession(this.getClass.getSimpleName)
    import spark.implicits._
    import org.apache.spark.sql.functions._

    // 2、提取原始，注册成临时表  【'pid, 'price, 'cat1, 'cat2, 'cat3】以及 【 split('kwds, " "】
    val items1: DataFrame = spark.read.option("header", true).csv("rec_system/data/ui_rate/item.profile.dat")
      .select('pid, 'price, 'cat1, 'cat2, 'cat3, split('kwds, " ").as("words")).drop("kwds")
    items1.createTempView("items")

    // 3、处理价格特征： 离散化
    val items: DataFrame = spark.sql(
      """
        |
        |select
        |pid,
        |case
        | when cast(price as double)<1000 and cat3='手机' then 0.0
        | when (cast(price as double) between 1000 and 2000) and cat3='手机' then 1.0
        | when (cast(price as double) between 2001 and 4000) and cat3='手机' then 2.0
        | when 4000<cast(price as double) and cat3='手机' then 3.0
        |
        | when cast(price as double)<100 and cat3='手环' then 0.0
        | when (cast(price as double) between 100 and 200 ) and cat3='手环' then 1.0
        | when (cast(price as double) between 201 and 400 ) and cat3='手环' then 2.0
        | when 400<cast(price as double) and cat3='手环' then 3.0
        |
        | when cast(price as double)<10 and cat3='排骨' then 0.0
        | when (cast(price as double) between 10 and 20 ) and cat3='排骨' then 1.0
        | when (cast(price as double) between 21 and 80 ) and cat3='排骨' then 2.0
        | when 80<cast(price as double) and cat3='排骨' then 3.0
        |
        |
        | when cast(price as double)<5 and cat3='坚果炒货' then 0.0
        | when (cast(price as double) between 5 and 10 ) and cat3='坚果炒货' then 1.0
        | when (cast(price as double) between 11 and 15 )  and cat3='坚果炒货' then 2.0
        | when 15<cast(price as double) and cat3='坚果炒货' then 3.0
        |end as level,
        |cat1,
        |cat2,
        |cat3,
        |words
        |
        |from items
        |
      """.stripMargin)


    /**
      * 4、数据处理；1）处理 cat1 ,cat2 ,cat3；2）处理 kwds
      *
      * pid,price,cat1 ,cat2 ,cat3 ,kwds
      * p01,6600 ,电子 ,数码 ,手机, Apple iPhone XR (A2108) 128GB 黑色 移动 联通 电信 4G手机 双卡双待
      */
    val idx1: StringIndexer = new StringIndexer()
      .setInputCol("cat1")
      .setOutputCol("c1")

    val idx2: StringIndexer = new StringIndexer()
      .setInputCol("cat2")
      .setOutputCol("c2")

    val idx3: StringIndexer = new StringIndexer()
      .setInputCol("cat3")
      .setOutputCol("c3")

    val idx1Df: DataFrame = idx1.fit(items).transform(items).drop("cat1")
    val idx2Df: DataFrame = idx2.fit(idx1Df).transform(idx1Df).drop("cat2")
    val idx3Df: DataFrame = idx3.fit(idx2Df).transform(idx2Df).drop("cat3")

    val hashingTF: HashingTF = new HashingTF().setInputCol("words").setNumFeatures(10000).setOutputCol("tf")
    val hashingDf: DataFrame = hashingTF.transform(idx3Df).drop("words")
    val idfDf: DataFrame = new IDF().setInputCol("tf").setOutputCol("idf").fit(hashingDf).transform(hashingDf).drop("tf")
    idfDf.show(10, false)

    // 5、将上面处理好的每个人的特征，整合成一个向量，注册为UDF函数 combineVec
    val combineVec: UserDefinedFunction = udf(
      (arr: mutable.WrappedArray[Double], vec: linalg.Vector) => {
        // 5-1、将向量先转回数组
        val vec1: Array[Double] = vec.toArray
        // 5-2、然后拼接两个数组
        val okVec: mutable.WrappedArray[Double] = arr.++(vec1)
        Vectors.dense(okVec.toArray).toSparse
      })

    // 6、整合所有特征为一个向量 ： （数组和向量）整合 as "features")
    val vecDf: DataFrame = idfDf.select('pid, combineVec(array('level, 'c1, 'c2, 'c3), 'idf).as("features"))
    vecDf.show(10, false)

    // 7、将物品和物品进行关联
    val joinedVec: DataFrame = vecDf.join(vecDf.toDF("pid2", "features2"), 'pid < 'pid2, "cross")

    // 8、求每两个物品之间的余弦相似度 ，UDF函数 cosSim
    val cosSim: UserDefinedFunction =
      udf(
        (v1: linalg.Vector, v2: linalg.Vector) => {
          val fenmu1: Double = v1.toArray.map(Math.pow(_, 2)).sum
          val fenmu2: Double = v2.toArray.map(Math.pow(_, 2)).sum

          val fenzi: Double = v1.toArray.zip(v2.toArray).map(tp => tp._1 * tp._2).sum
          fenzi / Math.pow(fenmu1 * fenmu2, 0.5)
        }
      )

    // 9、通过 余弦相似度 获取结果
    val itemSimilarity: DataFrame = joinedVec.select('pid, 'pid2, cosSim('features, 'features2) as "sim")
    itemSimilarity.show(50, false)
    itemSimilarity.coalesce(1).write.parquet("rec_system/data/cb_out/item_item")

    // 10、关闭Spark
    spark.close()
  }
}
