package demo.vector

import org.apache.spark.ml.linalg
import org.apache.spark.ml.linalg.Vectors

object Demo {

  def main(args: Array[String]): Unit = {
    // 測試數據
    val arr = Array(1.0, 2.0, 3.0, 4.0, 5.0, 6.0)

    //转为向量
    val vector: linalg.Vector = Vectors.dense(arr)

    def f(x: Int*) = {
      x.toArray.sum
    }

    f()

  }
}
