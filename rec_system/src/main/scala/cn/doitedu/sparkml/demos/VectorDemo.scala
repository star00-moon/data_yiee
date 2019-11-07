package cn.doitedu.sparkml.demos

import org.apache.spark.ml.linalg
import org.apache.spark.ml.linalg.{DenseVector, SparseVector, Vectors}

/**
  * @author: 余辉
  * @blog: https://blog.csdn.net/silentwolfyh
  * @create: 2019/10/21
  * @description:
  * 1、spark mllib 底层编程接口:Vector向量示例程序,欧氏距离和余弦距离实现
  **/
object VectorDemo {

  /** *
    * 使用向量的一些api，构造密集向量和稀疏向量
    */
  def constructVector(): Unit = {
    // org.apache.spark.ml.linalg.DenseVector
    val v1: linalg.Vector = Vectors.dense(Array(1.0, 3.0, 4.0, 2.0))
    //转为 array
    val arr: Array[Double] = v1.toArray
    //转为稀疏向量 toSparse
    val v1sp: SparseVector = v1.toSparse
    //转为稠密向量 toDense
    val v1dens: DenseVector = v1.toDense
    // apply获取第几个值
    val d: Double = v1.apply(2)
    // numActives 获取向量个数
    val actives: Int = v1.numActives
    // 找到第一个最大值的脚标
    val argmax: Int = v1.argmax

    println(arr.mkString(","))
    println(v1sp)
    println(v1dens)
    println(d)
    println(actives)
    println(argmax)
    println("-----------------------------")

    // 构造稀疏向量  调用向量的一些api ，sqdist(向量个数，向量位置，向量值)
    val v2: linalg.Vector = Vectors.sparse(100, Array(3, 8, 98), Array(1.0, 1.0, 2.0))
    //转为稠密向量 toDense
    val v2dense: DenseVector = v2.toDense
    //转为 array
    val arr2: Array[Double] = v2.toArray
    // apply获取第几个值
    val d2: Double = v2.apply(88)
    // numActives 获取向量个数
    val actives2: Int = v2.numActives

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

    // 表转为成稠密向量
    val v1: linalg.Vector = Vectors.dense(arr1.map(_.toDouble))
    val v2: linalg.Vector = Vectors.dense(arr2.map(_.toDouble))

    // 计算欧氏距离
    val sqd: Double = Vectors.sqdist(v1, v2) // sqdist 为平方差
    val eudist1: Double = Math.sqrt(sqd) //Math.sqrt 开根号
    val eudist2: Double = Math.pow(sqd, 1d / 2) // Math.pow开根号
    println(eudist1)
    println(eudist2)
  }

  /**
    * 自己写代码，计算向量之间的欧氏距离
    */
  def caclEuDistance2(): Unit = {
    // 表达成向量
    val v1: linalg.Vector = Vectors.dense(Array(1.0, 3, 5, 8))
    val v2: linalg.Vector = Vectors.dense(Array(2.0, 6, 10, 16))

    // 计算欧氏距离
    val sqdist: Double = v1.toArray.zip(v2.toArray).map(tp => Math.pow(tp._1 - tp._2, 2)).sum
    println(Math.pow(sqdist, 0.5))
  }

  /**
    * 自己写代码，计算向量之间的余弦距离
    */
  def caclCosineDistance(): Unit = {

    // 表达成向量
    val v1: linalg.Vector = Vectors.dense(Array(1.0, 3, 5, 8))
    val v2: linalg.Vector = Vectors.dense(Array(2.0, 6, 10, 16))

    val fenmu1: Double = v1.toArray.map(Math.pow(_, 2)).sum
    val fenmu2: Double = v2.toArray.map(Math.pow(_, 2)).sum
    val fenzi: Double = v1.toArray.zip(v2.toArray).map(tp => tp._1 * tp._2).sum
    val cosDist: Double = fenzi / Math.pow(fenmu1 * fenmu2, 0.5)
    println(cosDist)
  }

  def main(args: Array[String]): Unit = {
    //使用向量的一些api，构造密集向量和稀疏向量
    //        constructVector()

    // 使用Vectors工具类上的工具方法，计算向量之间的欧氏距离
    //    caclEuDistance()

    //自己写代码，计算向量之间的欧氏距离
    //            caclEuDistance2()

    // 自己写代码，计算向量之间的余弦距离
    caclCosineDistance()
  }
}
