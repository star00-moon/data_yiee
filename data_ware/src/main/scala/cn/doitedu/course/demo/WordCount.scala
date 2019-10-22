package cn.doitedu.course.demo

import cn.doitedu.commons.utils.SparkUtil
import org.apache.spark.rdd.RDD

/**
  * @author: 余辉
  * @blog: https://blog.csdn.net/silentwolfyh
  * @create: 2019/10/22
  * @description:
  * stage是什么？
  * -- 整个运算逻辑计划中，可以被放在一个task中完成的一段逻辑
  * -- 整个运算逻辑计划，以shuffle为分界点，被划分成多个逻辑段：stage
  *
  * taskset是什么？
  * -- taskset是指一组task，这一组task中的逻辑都相同，对应着DAG中的某个stage
  * -- taskset中的每一个task，逻辑虽然相同，但是处理的数据片不同
  *
  * task是什么？
  * -- task是真正处理数据的一个最小运行单元，往往对应着一个线程，对应着一个数据分片，对应着一个stage中的运算逻辑
  *
  * 并行度啥意思？
  * -- 同一时间，做同一个逻辑运算的执行者个数！
  *
  * 任务分片啥意思？
  * -- 给不同的任务执行者指定特定的数据范围，以便于让各个并行的task互相之间不冲突
  * -- 在mapreduce中，任务分片用InputSplit描述；
  * -- 在spark中，任务分片用Partition描述；
  *
  * partition啥意思
  * -- 用于为task分任务的一种描述，在spark中叫partition
  *
  * shuffle啥意思？
  * -- 一组task的数据需要洗牌分成多堆，然后传递给下游的一组task去聚合处理，这个过程就叫shuffle
  * -- 为什么（什么时候）需要shuffle？
  * -- 某个逻辑（比如分组累加），往往需要从多个上游task中获取正确数据（而且无法实现一一对应），则需要shuffle（宽依赖）
  *
  * executore是什么？
  * -- 一个进程，它是启动task线程的程序实体
  *
  * shuffleMapTask 是啥？
  * -- 就是上文中所说的task的一个具体种类！
  * -- 它可以从上游shuffle出来的数据中拉取属于自己处理的数
  * -- 它也可以为下游的task来shuffle分堆数据，提供下游去拉取
  *
  * resultTask是啥？
  * -- 它可以从上游shuffle中拉取数据并计算
  * -- 它还可以将计算结果写入外部存储系统
  **/
object WordCount {

  def main(args: Array[String]): Unit = {

    val spark = SparkUtil.getSparkSession()

    // 这句话读文件了吗？ 没读？ 那它做什么了？
    val rdd = spark.sparkContext.textFile("data_ware/demodata/a.txt")

    val rdd2: RDD[Array[String]] = rdd.map(line => line.split(" "))

    val rdd3: RDD[String] = rdd2.flatMap(arr => arr)

    val rdd4: RDD[(String, Int)] = rdd3.map(w => (w, 1))

    val rdd5 = rdd4.reduceByKey(_ + _) // 内含上一个阶段的task 将数据分给下游的不同task了

    val rdd6 = rdd5.map(tp => (tp._1.toUpperCase(), tp._2))


  }

}
