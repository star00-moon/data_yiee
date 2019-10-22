package cn.doitedu.sparkml.fpgrowth

import cn.doitedu.commons.utils.SparkUtil
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.fpm.FPGrowth

/**
  * @author: 余辉
  * @blog: https://blog.csdn.net/silentwolfyh
  * @create: 2019/10/21
  * @description: 基于关联规则分析算法fp-growth进行推荐计算
  **/
object RecFpGrowth {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)

    val spark = SparkUtil.getSparkSession(this.getClass.getSimpleName)
    import spark.implicits._

    val sample_ds = spark.read.textFile("rec_system/data/fpgrowth_data/input/sample.dat")
    val items = sample_ds.map(line => line.split(" ")).toDF("items")

    val fp = new FPGrowth()
      .setItemsCol("items")
      .setMinSupport(0.2)
      .setMinConfidence(0.1)

    val model = fp.fit(items)

    // 打印模型中已经算出来的：频繁项集 (支持度>0.4)
    model.freqItemsets.show(50, false)

    // 打印模型中已经算出来的：关联规则 （置信度>0.3)
    model.associationRules.show(50, false)

    /**
      * 用训练好的模型，来对其他用户（商品组合）做推荐
      */
    val test_ds = spark.read.textFile("rec_system/data/fpgrowth_data/input/test.data")
    val items_test = test_ds.map(line => line.split(" ")).toDF("items")

    val rec = model.transform(items_test)
    rec.show(50, false)

    spark.close()
  }
}
