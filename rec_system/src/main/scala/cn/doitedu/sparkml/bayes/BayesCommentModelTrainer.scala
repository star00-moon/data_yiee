package cn.doitedu.sparkml.bayes

import cn.doitedu.commons.utils.SparkUtil
import com.hankcs.hanlp.HanLP
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.classification.NaiveBayes
import org.apache.spark.ml.feature.{HashingTF, IDF}

/**
 * @author: 余辉
 * @blog:   https://blog.csdn.net/silentwolfyh
 * @create: 2019/10/21
 * @description:  评论分类贝叶斯模型训练器
 **/
object BayesCommentModelTrainer {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    val spark = SparkUtil.getSparkSession(this.getClass.getSimpleName)
    import spark.implicits._

    val poor = spark.read.textFile("G:\\testdata\\comment\\poor")
    val general = spark.read.textFile("G:\\testdata\\comment\\general")
    val good = spark.read.textFile("G:\\testdata\\comment\\good")

    val p = poor.map(line => (0.0, line))
    val z = general.map(line => (1.0, line))
    val g = poor.map(line => (2.0, line))

    val sample = p.union(z).union(g)

    // 把文本分词
    val words = sample
      .map(tp => {
        val label = tp._1
        val line = tp._2
        import scala.collection.JavaConversions._
        val words = HanLP.segment(line).map(term => term.word)
        (label, words)
      })
      .toDF("label","words")

    // 把词数组向量化
    val hashingTF = new HashingTF()
      .setInputCol("words")
      .setOutputCol("tf")
      .setNumFeatures(1000000)
    val tf = hashingTF.transform(words)

    // 加载训练器IDF，设置输入列为 tf，输出列为 idf
    val idf = new IDF()
      .setInputCol("tf")
      .setOutputCol("idf")
    val vec = idf.fit(tf).transform(tf)

    // 构造一个bayes算法工具
    val bayes = new NaiveBayes()
      .setLabelCol("label")
      .setFeaturesCol("idf")
      .setSmoothing(1)

    // 训练模型
    val model = bayes.fit(vec)

    model.save("rec_system/data/comment_bayes_model")

    spark.close()
  }
}
