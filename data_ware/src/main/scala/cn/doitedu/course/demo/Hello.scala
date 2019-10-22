package cn.doitedu.course.demo

import cn.doitedu.commons.utils.SparkUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Encoders, Row}
import org.apache.spark.sql.types.{DataTypes, StructType}

import scala.collection.immutable.HashMap

object Hello {

  def main(args: Array[String]): Unit = {
    val spark = SparkUtil.getSparkSession()
    import spark.implicits._


    val ds = spark.createDataset(Seq("a", "b", "c"))
    val df = ds.toDF("v")

    val schema = df.schema
    val schema2: StructType = schema.add("v2", DataTypes.StringType).add("v3", DataTypes.StringType)

    val rowrdd: RDD[Row] = df.rdd.map(row => {
      val values = row.toSeq.toList
      val newvalues = values.++:(Array("x", "y")).toArray

      Row(newvalues: _*)
    })

    val x = spark.createDataFrame(rowrdd, schema2)
    x.printSchema()
    x.show(10, false)

    val rdd = spark.sparkContext.makeRDD(Seq(HashMap("a" -> "1"), HashMap("b" -> "2")))
    import org.apache.spark.sql.Encoder
    import org.apache.spark.sql.Encoders
    import org.apache.spark.sql.types._
    import org.apache.spark.sql.catalyst.encoders._


    val encoder = Encoders.kryo(classOf[HashMap[String, String]])
    val dse = spark.createDataset(rdd)(encoder)
    dse.show(10, false)
    dse.map(mp=>mp.get("a").toString)
      .show(10,false)





    import spark.implicits._
    import org.apache.spark.sql.Encoder
    import org.apache.spark.sql.Encoders
    import org.apache.spark.sql.catalyst.encoders.RowEncoder
    import org.apache.spark.sql.types._
    /*val encoder = Encoders.tuple(
      newMapEncoder[Map[String, Int]],
      newIntArrayEncoder,
      Encoders.STRING,
      RowEncoder(
        StructType(
          Seq(
            StructField("num", IntegerType),
            StructField("str", StringType)
          )
        )
      )
    )*/

    spark.close()


  }

}
