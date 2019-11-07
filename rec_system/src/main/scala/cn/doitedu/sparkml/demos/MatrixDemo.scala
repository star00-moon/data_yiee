package cn.doitedu.sparkml.demos

import cn.doitedu.commons.utils.SparkUtil
import org.apache.spark.mllib.linalg
import org.apache.spark.mllib.linalg.{Matrix, Vectors}
import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, IndexedRow, IndexedRowMatrix, MatrixEntry, RowMatrix}
import org.apache.spark.mllib.stat.MultivariateStatisticalSummary
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
  * @author: 余辉
  * @blog: https://blog.csdn.net/silentwolfyh
  * @create: 2019/10/21
  * @description:
  * 1、spark mllib底层编程接口：矩阵Matrix
  * 2、面向行的分布式矩阵(RowMatrix)
  * 3、行索引矩阵(IndexedRowMatrix)
  * 4、三元组矩阵(CoordinateMatrix)
  *
  * 参考：
  * 1、https://yuhui.blog.csdn.net/article/details/102932981
  * 2、https://yuhui.blog.csdn.net/article/details/102933066
  **/
object MatrixDemo {

  /**
    * 面向行的分布式矩阵(RowMatrix)
    */
  def rowMatrix() = {

    // 1、建立session连接
    val spark: SparkSession = SparkUtil.getSparkSession(this.getClass.getSimpleName)
    import spark.implicits._

    //读取原始数据 ， header 和 inferSchema
    val df: DataFrame = spark.read
      .option("header", true)
      .option("inferSchema", true)
      .csv("rec_system/demodata/vecdata/vec.csv")

    // 将数据向量化 Vectors.dense
    val rdd: RDD[linalg.Vector] = df.rdd.map({
      case Row(id: Int, sex: Double, age: Double, height: Double, weight: Double, salary: Double)
      =>
        Vectors.dense(sex, age, height, weight, salary)
    })

    // 将向量rdd转成 Matrix
    val rowMatrix = new RowMatrix(rdd)

    // .rows就是将矩阵中的每一行取出来，得到的就是原来的： 向量RDD
    rowMatrix.rows.foreach(println)

    println("----------------------------")

    // 调矩阵的写好的算法api -- 求矩阵中各向量之间的协方差
    val comat: Matrix = rowMatrix.computeCovariance()
    val rowit: Iterator[linalg.Vector] = comat.rowIter
    rowit.foreach(println)

    // 求矩阵中的列统计指标（最大值，最小值，平均值.......)
    val summary: MultivariateStatisticalSummary = rowMatrix.computeColumnSummaryStatistics()
    println(summary.min) // 每一列的最小值
    println(summary.max) // 每一列的最大值
    println(summary.variance) // 每一列的标准差

    // 计算矩阵中各个列之间的余弦相似度
    val similaryMat = rowMatrix.columnSimilarities()

    spark.close()
  }

  /**
    * 行索引矩阵(IndexedRowMatrix)
    */
  def indexedRowMatrix() = {

    val spark: SparkSession = SparkUtil.getSparkSession(this.getClass.getSimpleName)
    import spark.implicits._

    // 将数据向量化
    val df: DataFrame = spark.read
      .option("header", true)
      .option("inferSchema", true)
      .csv("rec_system/demodata/vecdata/vec.csv")

    // 向量转为 行索引矩阵
    val rdd: RDD[IndexedRow] = df.rdd.map({
      case Row(id: Int, sex: Double, age: Double, height: Double, weight: Double, salary: Double)
      =>
        val v = Vectors.dense(sex, age, height, weight, salary)
        // 将向量变成indexedRow
        val idxRow = new IndexedRow(id.toLong, v)
        idxRow
    })

    val idxMat = new IndexedRowMatrix(rdd)

    spark.close()
  }

  /**
    * 三元组矩阵(CoordinateMatrix)
    */
  def coordinateMatrix() = {

    val spark: SparkSession = SparkUtil.getSparkSession(this.getClass.getSimpleName)
    import spark.implicits._

    // 将数据向量化
    val df: DataFrame = spark.read
      .option("header", true)
      .option("inferSchema", true)
      .csv("rec_system/demodata/vecdata/vec.csv")

    // zipWithIndex  该函数将RDD中的元素和这个元素在RDD中的ID（索引号）组合成键/值对。
    val x: RDD[(Row, Long)] = df.rdd.zipWithIndex()
    x.foreach(println)

    // 将数组转为 MatrixEntry的rdd
    val rdd: RDD[MatrixEntry] = x.flatMap(tp => {
      // 一行数据 row
      val row: Row = tp._1
      //角标
      val i: Long = tp._2
      row match {
        case Row(id: Int, sex: Double, age: Double, height: Double, weight: Double, salary: Double)
        =>
          val arr = Array(sex, age, height, weight, salary)
          for (j <- 0 to arr.size - 1) yield new MatrixEntry(i, j, arr(j))
      }
    })

    // 将 MatrixEntry的rdd 转为  三元组矩阵(CoordinateMatrix)
    val coordinMat = new CoordinateMatrix(rdd)

    //打印输出
    coordinMat.entries.foreach(entry => println(entry.i, entry.j, entry.value))

    println("------------------------------------")

    // 转为行矩阵
    coordinMat.toRowMatrix()

    // 矩阵转置
    val transposedMat: CoordinateMatrix = coordinMat.transpose()

    transposedMat.entries.foreach(entry => println(entry.i, entry.j, entry.value))

    println("------------------------------------")

    // 转为 RowMatrix 打印
    transposedMat.toRowMatrix().rows.foreach(println)
    spark.close()
  }

  def main(args: Array[String]): Unit = {
    // 三元组矩阵(CoordinateMatrix)
//    coordinateMatrix()

    // 行索引矩阵(IndexedRowMatrix)
    indexedRowMatrix()

    // 面向行的分布式矩阵(RowMatrix)
//    rowMatrix()
  }
}
