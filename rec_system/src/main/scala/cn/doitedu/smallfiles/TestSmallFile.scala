package cn.doitedu.smallfiles

import cn.doitedu.commons.utils.SparkUtil
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.{CombineTextInputFormat, TextInputFormat}


/**
  * @author: 余辉
  * @blog: https://blog.csdn.net/silentwolfyh
  * @create: 2019/10/21
  * @description: 如何处理大量小文件
  **/
object TestSmallFile {

  def main(args: Array[String]): Unit = {

    val spark = SparkUtil.getSparkSession("")
    val path = "G:\\testdata\\knn\\trainingDigits"

    // 默认切片规则，一个切片128M=hdfs的blocksize大小，如果文件太小，则一个文件就是一个分区
    val textInRdd = spark.sparkContext.newAPIHadoopFile(
      path,
      classOf[TextInputFormat],
      classOf[LongWritable],
      classOf[Text]
    )

    // hadoop.textinputformat :1934
    println("hadoop.textinputformat :" + textInRdd.partitions.length)

    // 改变分区数的一种方式，这种方式只能调大分区数
    val rdd1 = spark.sparkContext.textFile(path, 4)

    // sparkcontext.textfile:  1934
    println("sparkcontext.textfile:  " + rdd1.partitions.length)

    // 改变分区数的第二种方式，减少分区数： 利用CombineTextInputFormat（它可以将多个小文件划分成一个任务片）
    // hadoop.combinetextinputformat :1
    val conf = new Configuration()
    conf.setLong("mapreduce.input.fileinputformat.split.minsize.per.node", 134217728)
    val combineRdd = spark.sparkContext.newAPIHadoopFile(
      path,
      classOf[CombineTextInputFormat],
      classOf[LongWritable],
      classOf[Text],
      conf
    )
    println("hadoop.combinetextinputformat :" + combineRdd.partitions.length)

    // 优化小文件分区数的第三种方式，别用底层SparkContext去读
    // SparkSession已经做了自动优化
    // sparksession.read.textFile : 61
    val ds = spark.read.textFile(path)
    println("sparksession.read.textFile : " + ds.rdd.partitions.length)

    spark.close()
  }
}
