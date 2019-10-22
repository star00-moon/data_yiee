package cn.doitedu.sparkml.demos

import org.apache.spark.ml.linalg
import org.apache.spark.ml.linalg.{DenseVector, SparseVector, Vectors}

/**
  * @author: 余辉
  * @blog: https://blog.csdn.net/silentwolfyh
  * @create: 2019/10/21
  * @description:
  * 1、spark mllib 底层编程接口:Vector向量示例程序,欧氏距离和余弦距离实现
  *
  * 问题：数组为何转为向量，如果数组中有10个或者20个呢？
  **/
object VectorDemo {

  /***
    * 使用向量的一些api，构造密集向量和稀疏向量
    */
  def constructVector() = {
    // org.apache.spark.ml.linalg.DenseVector
    val v1: linalg.Vector = Vectors.dense(Array(1.0, 3.0, 4.0, 2.0))
    //转为 array
    val arr = v1.toArray
    //转为稀疏向量 sparse vector
    val v1sp: SparseVector = v1.toSparse
    //转为稠密向量 dense vecto
    val v1dens: DenseVector = v1.toDense
    // Gets the value of the ith element
    val d: Double = v1.apply(2)
    val actives = v1.numActives
    // 找到第一个最大值的脚标
    val argmax = v1.argmax

    println(arr.mkString(","))
    println(v1sp)
    println(v1dens)
    println(d)
    println(actives)
    println("-----------------------------")

    // 构造稀疏向量  调用向量的一些api
    val v2 = Vectors.sparse(100, Array(3, 8, 98), Array(1.0, 1.0, 2.0))
    val v2dense = v2.toDense
    val arr2 = v2.toArray
    val d2 = v2.apply(88)
    val actives2 = v2.numActives

    println(v2dense)
    println(arr2.mkString(","))
    println(d2)
    println(actives2)
  }

  /**
    * 使用Vectors工具类上的工具方法，计算向量之间的欧氏距离
    */
  def caclEuDistance(): Unit = {
    // 已存在数据是两个数组
    var arr1 = Array(1, 3, 5, 8)
    var arr2 = Array(2, 6, 10, 16)

    // 表转为成向量
    val v1 = Vectors.dense(arr1.map(_.toDouble))
    val v2 = Vectors.dense(arr2.map(_.toDouble))

    // 计算欧氏距离
    val sqd = Vectors.sqdist(v1, v2)
    val eudist1 = Math.sqrt(sqd) //Math.sqrt 开根号
    val eudist2 = Math.pow(sqd, 1d / 2) // Math.pow开根号
    println(eudist1)
    println(eudist2)
  }

  /**
    * 自己写代码，计算向量之间的欧氏距离
    */
  def caclEuDistance2(): Unit = {
    // 表达成向量
    val v1 = Vectors.dense(Array(1.0, 3, 5, 8))
    val v2 = Vectors.dense(Array(2.0, 6, 10, 16))
    //Array((1.0,2.0),(3,6),(5,10),(8,16)) =>  Array(1,9,25,64) =>

    // 计算欧氏距离
    val sqdist = v1.toArray.zip(v2.toArray).map(tp => Math.pow(tp._1 - tp._2, 2)).sum
    println(Math.pow(sqdist, 0.5))
  }

  /**
    * 自己写代码，计算向量之间的余弦距离
    */
  def caclCosineDistance(): Unit = {

    // 表达成向量
    val v1 = Vectors.dense(Array(1.0, 3, 5, 8))
    val v2 = Vectors.dense(Array(2.0, 6, 10, 16))

    val fenmu1 = v1.toArray.map(Math.pow(_, 2)).sum
    val fenmu2 = v2.toArray.map(Math.pow(_, 2)).sum
    val fenzi = v1.toArray.zip(v2.toArray).map(tp => tp._1 * tp._2).sum
    val cosDist = fenzi / Math.pow(fenmu1 * fenmu2, 0.5)
    println(cosDist)
  }

  def main(args: Array[String]): Unit = {
    //    constructVector()
    caclEuDistance()
    //    caclEuDistance2()
    //    caclCosineDistance()
  }
}
