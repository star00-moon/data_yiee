import cn.doitedu.commons.utils.SparkUtil
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.sql.{Dataset, Row}

/**
 * @author: 余辉 
 * @blog:   https://blog.csdn.net/silentwolfyh
 * @create: 2019/10/22
 * @description: 
 **/
object GenderPre {

  def main(args: Array[String]): Unit = {
    val spark = SparkUtil.getSparkSession(this.getClass.getSimpleName)
    import spark.implicits._

    val dfsample = spark.read.option("header", true).option("inferSchema", true).csv("rec_system/data/lgreg/lgreg_sample.csv")
    dfsample.printSchema()

    val dftest = spark.read.option("header", true).option("inferSchema", true).csv("rec_system/data/lgreg/lgreg_test.csv")
    dftest.printSchema()


    val vecsample = dfsample.map({
      case Row(label: Double, gid: Int, category1: Double, category2: Double, category3: Double, brand1: Double, brand2: Double, brand3: Double, day30_buy_cnts: Double, day30_buy_amt: Double)
      => {
        val fts = Array(category1, category2, category3, brand1, brand2, brand3, day30_buy_cnts, day30_buy_amt)
        (gid, label, Vectors.dense(fts))
      }
    }).toDF("gid", "label", "features")

    vecsample.show(10, false)

    val vectest = dftest.map({
      case Row(label: Double, gid: Int, category1: Double, category2: Double, category3: Double, brand1: Double, brand2: Double, brand3: Double, day30_buy_cnts: Double, day30_buy_amt: Double)
      => {
        val fts = Array(category1, category2, category3, brand1, brand2, brand3, day30_buy_cnts, day30_buy_amt)
        (gid, label, Vectors.dense(fts))
      }
    }).toDF("gid", "label", "features")

    val lg = new LogisticRegression()
      .setRegParam(0.5)
      .setLabelCol("label")
      .setFeaturesCol("features")

    val res = lg.fit(vecsample).transform(vectest)
    res.show(10, false)


    val rd = res.rdd.map(row => (row(1).asInstanceOf[Double], row(5).asInstanceOf[Double]))
    rd.take(10).foreach(println)

    /**
      * 算法评估
      */
    val mc = new MulticlassMetrics(rd)
    // 准确度
    println(mc.accuracy)
    // 精确度
    println(mc.precision(1.0))
    // 混淆矩阵
    val iter = mc.confusionMatrix.rowIter
    iter.foreach(println)
    // 召回率
    println(mc.recall(1.0))

    spark.close()
  }
}
