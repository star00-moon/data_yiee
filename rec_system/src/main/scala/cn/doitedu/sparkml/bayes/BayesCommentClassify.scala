package cn.doitedu.sparkml.bayes

import cn.doitedu.commons.utils.SparkUtil
import com.hankcs.hanlp.HanLP
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.classification.NaiveBayesModel
import org.apache.spark.ml.feature.{HashingTF, IDF}

/**
  * @author: 余辉
  * @blog: https://blog.csdn.net/silentwolfyh
  * @create: 2019/10/21
  * @description: 用训练好的bayes模型来预测未知类别的评论
  **/
object BayesCommentClassify {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    val spark = SparkUtil.getSparkSession(this.getClass.getSimpleName)
    import spark.implicits._

    val df = spark.read.option("header", true).csv("rec_system/data/test_comment/u.comment.dat")

    val words = df.map(row => {
      val gid = row.getAs[String]("gid")
      val pid = row.getAs[String]("pid")
      val comment = row.getAs[String]("comment")
      import scala.collection.JavaConversions._
      val words = HanLP.segment(comment).map(term => term.word)
      (gid, pid, words)
    }).toDF("gid", "pid", "words")

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

    val model = NaiveBayesModel.load("rec_system/data/comment_bayes_model")
    val predict = model.transform(vec).drop("tf").drop("idf")
    predict.show(10, false)
    spark.close()
  }
}