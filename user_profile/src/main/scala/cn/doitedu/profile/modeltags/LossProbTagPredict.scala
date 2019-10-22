package cn.doitedu.profile.modeltags

import cn.doitedu.commons.utils.SparkUtil
import org.apache.spark.ml.classification.NaiveBayesModel
import org.apache.spark.ml.linalg
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

/**
 * @author: 余辉 
 * @blog:   https://blog.csdn.net/silentwolfyh
 * @create: 2019/10/20
 * @description: 
 **/
object LossProbTagPredict {

  def main(args: Array[String]): Unit = {

    // 1、建立session连接  import spark.implicits._
    val spark = SparkUtil.getSparkSession(this.getClass.getSimpleName)
    import spark.implicits._

    // 2、建立Schema
    val schema2 = new StructType(Array(
      new StructField("gid", DataTypes.DoubleType),
      new StructField("3_cs", DataTypes.DoubleType),
      new StructField("15_cs", DataTypes.DoubleType),
      new StructField("3_xf", DataTypes.DoubleType),
      new StructField("15_xf", DataTypes.DoubleType),
      new StructField("3_th", DataTypes.DoubleType),
      new StructField("15_th", DataTypes.DoubleType),
      new StructField("3_hp", DataTypes.DoubleType),
      new StructField("15_hp", DataTypes.DoubleType),
      new StructField("3_cp", DataTypes.DoubleType),
      new StructField("15_cp", DataTypes.DoubleType),
      new StructField("last_dl", DataTypes.DoubleType),
      new StructField("last_xf", DataTypes.DoubleType)
    ))

    // 3、从数组转为vector
    import org.apache.spark.sql.functions._
    val toVec = (cs3: Double, cs15: Double, xf3: Double, xf15: Double, th3: Double, th15: Double, hp3: Double, hp15: Double, cp3: Double, cp15: Double, last_dl: Double, last_xf: Double) => {
      Vectors.dense(Array(cs3, cs15, xf3, xf15, th3, th15, hp3, hp15, cp3, cp15, last_dl, last_xf))
    }

    // 4、注册成一个UDF函数，将组转为vector
    spark.udf.register("to_vec", toVec)

    // 5、加载测试数据
    val testData = spark.read.option("header", true).schema(schema2).csv("user_profile/data/modeltags/lossprob/modeltag_test")

    // 6、将测试数据转换成特征向量集合，机器学习算法中的，特征数据，要封装在一个Vector向量类型中
    val testFeatures = testData.selectExpr("gid", "to_vec(3_cs,15_cs,3_xf,15_xf,3_th,15_th,3_hp,15_hp,3_cp,15_cp,last_dl,last_xf) as feature")

    // 7、加载之前训练好的贝叶斯模型
    val model = NaiveBayesModel.load("user_profile/data/modeltags/bayes_module")

    // 8、用之前训练好的贝叶斯模型来预测未知数据，生成流失概率预测标签
    val pre = model.transform(testFeatures)
    pre.show(10, false)

    // 9、注册成一个UDF函数，将vector转为数组
    val getProb = (vec: linalg.Vector) => {
      vec.toArray(1)
    }
    spark.udf.register("getProb", getProb)

    // 10、将预测结果生成标签
    pre.selectExpr("gid", "'M014'", "'T143'", "getProb(prob) as lossprob", "cast(-9999.9 as double) as weight")
      .show(10, false)

    // 11、关闭spark
    spark.close()
  }
}
