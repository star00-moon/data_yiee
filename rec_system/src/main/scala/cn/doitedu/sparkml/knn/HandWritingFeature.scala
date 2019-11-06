package cn.doitedu.sparkml.knn

import cn.doitedu.commons.utils.SparkUtil
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.{CombineFileSplit, CombineTextInputFormat, FileSplit, TextInputFormat}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.{NewHadoopRDD, RDD}
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ListBuffer


/**
  * @author: 余辉
  * @blog: https://blog.csdn.net/silentwolfyh
  * @create: 2019/10/21
  * @description: 手写数字识别算法
  **/
object HandWritingFeature {

  def main(args: Array[String]): Unit = {

    // 1、建立session连接
    Logger.getLogger("org").setLevel(Level.WARN)
    val spark: SparkSession = SparkUtil.getSparkSession(this.getClass.getSimpleName)

    // 2、样本数据集
    //val path = "G:\\testdata\\knn\\trainingDigits"
    val path = "G:\\testdata\\knn\\\\testDigits"

    // 3、通过Hadoop的newAPIHadoopFile读取数据   参考：路径，输入类型，偏移量类型，文本类型
    val rdd: RDD[(LongWritable, Text)] = spark.sparkContext.newAPIHadoopFile(path, classOf[TextInputFormat], classOf[LongWritable], classOf[Text])
    val hdpRdd: NewHadoopRDD[LongWritable, Text] = rdd.asInstanceOf[NewHadoopRDD[LongWritable, Text]]
    println("原始分区数： " + hdpRdd.partitions.length)

    /**
      * 4、将原始的训练数据，变成一个样本一条向量及其类别标签
      */
    val vecs: RDD[(String, String, Array[Double])] = hdpRdd.mapPartitionsWithInputSplit((split, iter) => {
      // 4-1、所属文件
      val filesplit: FileSplit = split.asInstanceOf[FileSplit]
      // 4-2、文件名称
      val filename: String = filesplit.getPath.getName
      // 4-3、文件名称的第一个值
      val label: String = filename.split("_")(0)
      // 4-4、建立一个ListBuffer
      val lines = new ListBuffer[String]
      // 4-5、将该分区（某文件）的每一行，装入一个list
      iter.foreach(tp => (lines += tp._2.toString))
      val vecArray: Array[Double] = lines.filter(StringUtils.isNoneBlank(_)).mkString("").map(char => char.toString.toDouble).toArray
      // 4-6、List((filename, label, vecArray)) 转为迭代器
      List((filename, label, vecArray)).toIterator
    }).coalesce(1)

    println("处理之后的分区数： " + vecs.partitions.length)

    // 5、保存
    //vecs.take(10).foreach(tp=>println((tp._1,tp._2.mkString(","))))
    vecs.map(tp => tp._1 + "\001" + tp._2 + "\002" + tp._3.mkString(",")).saveAsTextFile("rec_system/demodata/digitTestVec")

    // 6、关闭spark
    spark.close()
  }
}