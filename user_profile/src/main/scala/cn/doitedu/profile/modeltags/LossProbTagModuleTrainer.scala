package cn.doitedu.profile.modeltags

import java.io.File

import cn.doitedu.commons.utils.{FileUtils, SparkUtil}
import org.apache.spark.ml.classification.NaiveBayes
import org.apache.spark.ml.linalg
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}


/**
  * @author: 余辉
  * @blog: https://blog.csdn.net/silentwolfyh
  * @create: 2019/10/20
  * @description: 模型训练器
  *               流失率预测标签计算
  *               算法：朴素贝叶斯  naive bayes
  **/
object LossProbTagModuleTrainer {

  def main(args: Array[String]): Unit = {

    // 1、建立session连接  import spark.implicits._
    val spark: SparkSession = SparkUtil.getSparkSession(this.getClass.getSimpleName)
    import spark.implicits._

    // 2、建立Schema   便签为：label,gid,3_cs,15_cs,3_xf,15_xf,3_th,15_th,3_hp,15_hp,3_cp,15_cp,last_dl,last_xf
    val schema = new StructType(Array(
      new StructField("label", DataTypes.DoubleType),
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

    // 3、加载模型数据
    val df: DataFrame = spark.read.schema(schema).option("header", true).csv("user_profile/data/modeltags/lossprob/modeltag_sample")

    // 4、从数组转为vector
    import org.apache.spark.sql.functions._
    val toVec = (cs3: Double, cs15: Double, xf3: Double, xf15: Double, th3: Double, th15: Double, hp3: Double, hp15: Double, cp3: Double, cp15: Double, last_dl: Double, last_xf: Double) => {
      Vectors.dense(Array(cs3, cs15, xf3, xf15, th3, th15, hp3, hp15, cp3, cp15, last_dl, last_xf))
    }

    // 5、注册成一个UDF函数，将组转为vector
    spark.udf.register("to_vec", toVec)

    // 6、将测试数据转换成特征向量集合，机器学习算法中的，特征数据，要封装在一个Vector向量类型中
    val sampleFeatureDataFrame = df.selectExpr("label", "to_vec(3_cs,15_cs,3_xf,15_xf,3_th,15_th,3_hp,15_hp,3_cp,15_cp,last_dl,last_xf) as feature")
    sampleFeatureDataFrame.show(10, false)

    // 7、加载之前训练好的贝叶斯模型
    val bayes = new NaiveBayes()
    bayes.setLabelCol("label")
      .setFeaturesCol("feature")
      .setSmoothing(0.1) //  拉普拉斯平滑系数
      .setProbabilityCol("prob") //probability
      .setPredictionCol("pre") // prediction

    // 8、用bayes算法来对经验数据训练模型
    val model = bayes.fit(sampleFeatureDataFrame)

    // 9、保存训练好的模型
    FileUtils.deleteDir(new File("user_profile/data/modeltags/bayes_module"))
    model.save("user_profile/data/modeltags/bayes_module")

    // 10、关闭spark
    spark.close()
  }
}
