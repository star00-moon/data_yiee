package cn.doitedu.profile.tagexport

import cn.doitedu.commons.utils.SparkUtil
import com.sun.mail.smtp.DigestMD5
import org.apache.commons.codec.digest.DigestUtils
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.client.{ConnectionFactory, TableDescriptor, TableDescriptorBuilder}
import org.apache.hadoop.hbase.{HBaseConfiguration, KeyValue, TableName}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat2, TableOutputFormat}
import org.apache.hadoop.hbase.tool.LoadIncrementalHFiles
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.sql.SparkSession

/**
  * @author: 余辉
  * @blog: https://blog.csdn.net/silentwolfyh
  * @create: 2019/10/20
  * @description:
  * 利用spark将数据整理成表结构的形式，并生成hbase的底层文件hfile
  * 然后利用hbase提供的bulkloader api将hfile导入hbase
  * -- hbase 建表语句  > create 'profile_tags','f'
  **/
object ProfileTags2Hbase {

  def main(args: Array[String]): Unit = {

    // val date  = args(0)
    val date: String = "2019-06-16"

    // 1、建立spark连接，本地模式       import spark.implicits._
    val spark = SparkSession.builder().appName("")
      .master("local")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()
    import spark.implicits._

    // 2、加载明细标签数据 user_profile/data/output/tag_merge/day02
    val tagsDF = spark.read.parquet("user_profile/data/output/tag_merge/day02")
    tagsDF.printSchema()

    /**
      * root
      * |-- gid: long (nullable = true)
      * |-- tag_module: string (nullable = true)
      * |-- tag_name: string (nullable = true)
      * |-- tag_value: string (nullable = true)
      * |-- weight: double (nullable = true)
      */

    tagsDF.show(50, false)

    // 3、Hbase参数配置
    val conf = HBaseConfiguration.create()
    // 3-1、zK地址
    conf.set("hbase.zookeeper.quorum", "hadoop11:2181,hadoop12:2181,hadoop13:2181")
    // 3-2、设置输出hbase的表
    conf.set(TableOutputFormat.OUTPUT_TABLE, "profile_tags")
    // 3-3、hdfs默認文件
    conf.set("fs.defaultFS", "hdfs://hadoop11:9000/")

    // 4、指定的其实就是rowkey类型
    val job = Job.getInstance(conf)
    job.setMapOutputKeyClass(classOf[ImmutableBytesWritable]) // 指定的其实就是rowkey类型
    job.setMapOutputValueClass(classOf[KeyValue])

    val tableDesc = TableDescriptorBuilder.newBuilder(TableName.valueOf("profile_tags")).build()
    HFileOutputFormat2.configureIncrementalLoadMap(job, tableDesc)

    /**
      * 整理成 (k,v) 元组 ，
      * k（就是hbase表中的rowkye）是 ImmutableBytesWritable类型，
      * v（就是hbase表中的一个qualifier+value=> cell）是 KeyValue类型
      */
    val kvRdd = tagsDF.rdd.map(row => {

      val gid = row.getAs[Long]("gid").toString
      val gidmd5 = DigestUtils.md5Hex(gid + "").substring(0, 10) + date
      val tag_module = row.getAs[String]("tag_module")
      val tag_name = row.getAs[String]("tag_name")
      val tag_value = row.getAs[String]("tag_value")
      val weight = row.getAs[Double]("weight")
      (gidmd5, tag_module, tag_name, tag_value, weight)
    })
      // 对数据按hbase的要求排序： 先按rowkey，再按列族，再按qualifier
      .sortBy(tp => (tp._1, tp._2, tp._3, tp._4))
      .map(tp => {
        // KeyValue(key，列族，列名，值)
        val keyvalue = new KeyValue(tp._1.getBytes(), "f".getBytes, (tp._2 + ":" + tp._3 + ":" + tp._4).getBytes, Bytes.toBytes(tp._5))
        // rowkey: ImmutableBytesWritable
        val rowkey = new ImmutableBytesWritable(tp._1.getBytes())
        (rowkey, keyvalue)
      })

    // 将RDD[(K,V)]利用HFileOutputFormat2存储为HFile文件
    kvRdd.saveAsNewAPIHadoopFile("hdfs://hadoop11:9000/tmp/taghfile/2019-06-16",
      classOf[ImmutableBytesWritable],
      classOf[KeyValue],
      classOf[HFileOutputFormat2],
      job.getConfiguration
    )

    spark.close()

    println("hfile 文件 生成完毕 -----------------------")

    // 利用hbase提供的 LoadIncrementalHFiles.doBulkload() 来将Hfile导入hbase
    val conn = ConnectionFactory.createConnection(conf)
    val admin = conn.getAdmin
    val table = conn.getTable(TableName.valueOf("profile_tags"))
    val locator = conn.getRegionLocator(TableName.valueOf("profile_tags"))

    val loader = new LoadIncrementalHFiles(conf)
    loader.doBulkLoad(new Path(("hdfs://hadoop11:9000/tmp/taghfile/2019-06-16")), admin, table, locator)

    println("恭喜你，hfile数据导入完成，你可以去hbase上查询数据了 -----------------------")
  }
}
