import cn.doitedu.commons.utils.SparkUtil
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.feature.Bucketizer
import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, IndexedRow, IndexedRowMatrix, MatrixEntry, RowMatrix}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.apache.spark.sql.{Dataset, Row, RowFactory}
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.stat.MultivariateStatisticalSummary

import scala.collection.{immutable, mutable}

object MatrixDemo {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.WARN)
    val spark = SparkUtil.getSparkSession("")
    import spark.implicits._
    import org.apache.spark.sql.functions._

    val df = spark.read
      .option("header",true)
      .option("inferSchema",true)
      .csv("rec_system/data/common/some.csv")

    df.printSchema()
    df.show(10,false)

    val tovec = udf((arr:mutable.WrappedArray[Double])=> {Vectors.dense(arr.toArray)})

    val vecrdd: RDD[Vector] = df.rdd.map({
      case Row(a:Double,b:Double,c:Double,d:Double)
        =>
        Vectors.dense(Array(a,b,c,d))
    })

    val rm = new RowMatrix(vecrdd)
    rm.computeCovariance()
    val summary: MultivariateStatisticalSummary = rm.computeColumnSummaryStatistics()
    println(summary.count)
    println(summary.max)
    println(summary.min)
    println(summary.variance.numActives)

    println("-------------------")

    val rm2: RDD[MatrixEntry] = rm.rows.zipWithIndex().flatMap(tp=>{
      val v = tp._1.toArray
      val i = tp._2
      for(j <- 0 to v.size-1) yield new MatrixEntry(j,i,v(j))
    })

    val cm2 = new CoordinateMatrix(rm2)
    cm2.toRowMatrix().rows.take(10).foreach(println)

    println("-------------------")
    cm2.transpose().toRowMatrix().rows.map(v=>v.toArray.toList).foreach(println)
    println("-------------------")
    val coordinateMatrix = rm.columnSimilarities()
    coordinateMatrix.entries.toDF("i","j","value").show(20,false)

    println("-------------------")
    val idxrowrdd = vecrdd.zipWithIndex().map(tp => {
      val v = tp._1
      val i = tp._2
      new IndexedRow(Math.abs((i - 1)), v)
    })
    val idxm = new IndexedRowMatrix(idxrowrdd)
    idxm.rows.foreach(println)
    idxm.computeGramianMatrix()


    new Bucketizer().setInputCol("").setOutputCol("").setSplits(Array(1.0,2.0))

    spark.close()
  }
}
