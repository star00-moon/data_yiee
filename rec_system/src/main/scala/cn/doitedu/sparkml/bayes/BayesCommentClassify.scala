package cn.doitedu.sparkml.bayes

import cn.doitedu.commons.utils.SparkUtil
import com.hankcs.hanlp.HanLP
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.classification.NaiveBayesModel
import org.apache.spark.ml.feature.{HashingTF, IDF}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable

/**
  * @author: 余辉
  * @blog: https://blog.csdn.net/silentwolfyh
  * @create: 2019/10/21
  * @description: 用训练好的bayes模型来预测未知类别的评论
  **/
object BayesCommentClassify {

  def main(args: Array[String]): Unit = {
    // 1、建立session连接
    Logger.getLogger("org").setLevel(Level.WARN)
    val spark: SparkSession = SparkUtil.getSparkSession(this.getClass.getSimpleName)
    import spark.implicits._

    // 2、加载数据
    val df: DataFrame = spark.read.option("header", true).csv("rec_system/data/test_comment/u.comment.dat")

    // 3、数据转为DF  (gid, pid, words)
    val words: DataFrame = df.map(row => {
      val gid: String = row.getAs[String]("gid")
      val pid: String = row.getAs[String]("pid")
      val comment: String = row.getAs[String]("comment")
      import scala.collection.JavaConversions._
      val words: mutable.Buffer[String] = HanLP.segment(comment).map(term => term.word)
      (gid, pid, words)
    }).toDF("gid", "pid", "words")

    // 4、把词数组向量化,设置向量为1万
    val hashingTF: HashingTF = new HashingTF()
      .setInputCol("words")
      .setOutputCol("tf")
      .setNumFeatures(1000000)
    val tf: DataFrame = hashingTF.transform(words)

    // 5、加载训练器IDF，设置输入列为 tf，输出列为 idf
    val idf: IDF = new IDF()
      .setInputCol("tf")
      .setOutputCol("idf")
    val vec: DataFrame = idf.fit(tf).transform(tf)

    // 6、加载评论模型
    val model: NaiveBayesModel = NaiveBayesModel.load("rec_system/data/comment_bayes_model")
    val predict: DataFrame = model.transform(vec).drop("tf").drop("idf")
    predict.show(10, truncate = false)

    // 7、spark关闭
    spark.close()
  }
}