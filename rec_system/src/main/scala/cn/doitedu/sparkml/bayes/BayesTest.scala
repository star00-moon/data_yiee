package cn.doitedu.sparkml.bayes

import cn.doitedu.commons.utils.SparkUtil
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.classification.{NaiveBayes, NaiveBayesModel}
import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * @author: 余辉
  * @blog: https://blog.csdn.net/silentwolfyh
  * @create: 2019/10/21
  * @description: 文本分类，bayes示例程序
  **/
object BayesTest {

  def main(args: Array[String]): Unit = {

    // 1、建立session连接
    Logger.getLogger("org").setLevel(Level.WARN)
    val spark: SparkSession = SparkUtil.getSparkSession(this.getClass.getSimpleName)

    // 2、读取原始数据 ， header 和 inferSchema
    val df: DataFrame = spark.read.option("header", true).option("inferSchema", true).csv("rec_system/demodata/bayes_demo_data/sample.txt")

    // 3、通过Tokenizer进行分词，对doc列分词，放入words列
    val tokenizer: Tokenizer = new Tokenizer().setInputCol("doc").setOutputCol("words")

    // 4、将分词的数据转为数组
    val wordsDF: DataFrame = tokenizer.transform(df)

    // 5、加载训练器HashingTF，设置输入列 words ， 输出列 tf_vec ， 特征值为100
    val hashingTF: HashingTF = new HashingTF()
      .setInputCol("words")
      .setOutputCol("tf_vec")
      .setNumFeatures(100)

    // 6、数据进行训练器
    val tfvecDF: DataFrame = hashingTF.transform(wordsDF)
    tfvecDF.show(10, false)

    // 7、加载训练器IDF，设置输入列为tf_vec，输出列为tfidf_vec
    val idf: IDF = new IDF()
      .setInputCol("tf_vec")
      .setOutputCol("tfidf_vec")
    val idfvecDF: DataFrame = idf.fit(tfvecDF).transform(tfvecDF)

    idfvecDF.show(10, false)

    // 8、使用贝叶斯进行训练
    val bayes: NaiveBayes = new NaiveBayes()
      .setLabelCol("label")
      .setFeaturesCol("tfidf_vec")
      .setSmoothing(1)

    val model: NaiveBayesModel = bayes.fit(idfvecDF)
    model.save("")

    spark.close()
  }
}
