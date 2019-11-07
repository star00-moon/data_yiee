package cn.doitedu.sparkml.fpgrowth

import cn.doitedu.commons.utils.SparkUtil
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.fpm.{FPGrowth, FPGrowthModel}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  * @author: 余辉
  * @blog: https://blog.csdn.net/silentwolfyh
  * @create: 2019/10/21
  * @description: 基于关联规则分析算法fp-growth进行推荐计算
  **/
object RecFpGrowth {
  def main(args: Array[String]): Unit = {
    // 1、建立session连接
    Logger.getLogger("org").setLevel(Level.WARN)
    val spark: SparkSession = SparkUtil.getSparkSession(this.getClass.getSimpleName)
    import spark.implicits._

    // 2、获取原始数据，items切分之后，转为DF
    val sample_ds: Dataset[String] = spark.read.textFile("rec_system/data/fpgrowth_data/input/sample.dat")
    val items: DataFrame = sample_ds.map(line => line.split(" ")).toDF("items")

    // 3、调用FPGrowth算法
    val fp: FPGrowth = new FPGrowth()
      .setItemsCol("items")
      .setMinSupport(0.2)
      .setMinConfidence(0.1)

    // 4、FPGrowth训练模型
    val model: FPGrowthModel = fp.fit(items)

    // 5、打印模型中已经算出来的：频繁项集 (支持度>0.4)
    model.freqItemsets.show(50, false)

    // 6、打印模型中已经算出来的：关联规则 （置信度>0.3)
    model.associationRules.show(50, false)

    // 7、用训练好的模型，来对其他用户（商品组合）做推荐
    val test_ds: Dataset[String] = spark.read.textFile("rec_system/data/fpgrowth_data/input/test.data")
    val items_test: DataFrame = test_ds.map(line => line.split(" ")).toDF("items")

    // 8、使用模型，进行推测
    val rec: DataFrame = model.transform(items_test)
    rec.show(50, false)

    // 9、关闭Spark
    spark.close()
  }
}
