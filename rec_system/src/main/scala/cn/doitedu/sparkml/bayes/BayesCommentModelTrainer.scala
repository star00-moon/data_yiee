package cn.doitedu.sparkml.bayes

import cn.doitedu.commons.utils.SparkUtil
import com.hankcs.hanlp.HanLP
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.classification.{NaiveBayes, NaiveBayesModel}
import org.apache.spark.ml.feature.{HashingTF, IDF}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import scala.collection.mutable

/**
  * @author: 余辉
  * @blog: https://blog.csdn.net/silentwolfyh
  * @create: 2019/10/21
  * @description: 评论分类贝叶斯模型训练器
  **/
object BayesCommentModelTrainer {

  def main(args: Array[String]): Unit = {

    // 1、建立session连接
    Logger.getLogger("org").setLevel(Level.WARN)
    val spark: SparkSession = SparkUtil.getSparkSession(this.getClass.getSimpleName)
    import spark.implicits._

    // 2、加载数据
    val poor: Dataset[String] = spark.read.textFile("G:\\testdata\\comment\\poor")
    val general: Dataset[String] = spark.read.textFile("G:\\testdata\\comment\\general")
    val good: Dataset[String] = spark.read.textFile("G:\\testdata\\comment\\good")

    // 3、数据每行给权重
    val p: Dataset[(Double, String)] = poor.map(line => (0.0, line))
    val z: Dataset[(Double, String)] = general.map(line => (1.0, line))
    val g: Dataset[(Double, String)] = poor.map(line => (2.0, line))

    // 4、数据进行union
    val sample: Dataset[(Double, String)] = p.union(z).union(g)

    // 5、把文本分词
    val words: DataFrame = sample
      .map(tp => {
        val label: Double = tp._1
        val line: String = tp._2
        import scala.collection.JavaConversions._
        val words: mutable.Buffer[String] = HanLP.segment(line).map(term => term.word)
        (label, words)
      })
      .toDF("label", "words")

    // 6、把词数组向量化
    val hashingTF: HashingTF = new HashingTF()
      .setInputCol("words")
      .setOutputCol("tf")
      .setNumFeatures(1000000)
    val tf: DataFrame = hashingTF.transform(words)

    // 7、加载训练器IDF，设置输入列为 tf，输出列为 idf
    val idf: IDF = new IDF()
      .setInputCol("tf")
      .setOutputCol("idf")
    val vec: DataFrame = idf.fit(tf).transform(tf)

    // 8、构造一个bayes算法工具
    val bayes: NaiveBayes = new NaiveBayes()
      .setLabelCol("label")
      .setFeaturesCol("idf")
      .setSmoothing(1)

    // 9、训练模型，且保存
    val model: NaiveBayesModel = bayes.fit(vec)
    model.save("rec_system/data/comment_bayes_model")

    // 10、spark关闭
    spark.close()
  }
}
