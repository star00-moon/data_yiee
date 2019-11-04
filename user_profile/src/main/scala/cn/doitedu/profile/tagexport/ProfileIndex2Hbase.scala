package cn.doitedu.profile.tagexport

import org.apache.commons.codec.digest.DigestUtils
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.{HBaseConfiguration, KeyValue, TableName}
import org.apache.hadoop.hbase.client.{Admin, Connection, ConnectionFactory, RegionLocator, Table, TableDescriptor, TableDescriptorBuilder}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat2, TableOutputFormat}
import org.apache.hadoop.hbase.tool.LoadIncrementalHFiles
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}


/**
  * @author: 余辉
  * @blog: https://blog.csdn.net/silentwolfyh
  * @create: 2019/10/19
  * @description:
  * 1、画像数据的id->gid索引数据入库bulkload程序
  * 2、hbase> create 'IDX_PROFILE_ID_GID','f'
  **/

object ProfileIndex2Hbase {

  def main(args: Array[String]): Unit = {

    // val date  = args(0)
    val date: String = "2019-06-16"

    // 1、建立spark连接，本地模式       import spark.implicits._
    val spark = SparkSession.builder().appName("")
      .master("local")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()
    import spark.implicits._

    // 2、Hbase参数配置
    val conf2 = HBaseConfiguration.create()
    // 2-1、zK地址
    conf2.set("hbase.zookeeper.quorum", "hadoop11:2181,hadoop12:2181,hadoop13:2181")
    // 2-2、设置输出hbase的表
    conf2.set(TableOutputFormat.OUTPUT_TABLE, "IDX_PROFILE_ID_GID")
    // 2-3、hdfs默認文件
    conf2.set("fs.defaultFS", "hdfs://hadoop11:9000/")

    // 3、指定的其实就是rowkey类型
    val job2: Job = Job.getInstance(conf2)
    // 3-1、指定的其实就是rowkey类型
    job2.setMapOutputKeyClass(classOf[ImmutableBytesWritable])
    // 3-2、列簇中value的类型
    job2.setMapOutputValueClass(classOf[KeyValue])

    // 4、hbase的表描述
    val tableDesc2: TableDescriptor = TableDescriptorBuilder.newBuilder(TableName.valueOf("IDX_PROFILE_ID_GID")).build()
    HFileOutputFormat2.configureIncrementalLoadMap(job2, tableDesc2)

    // 5、加载 idmp ， 路径：user_profile/data/output/tag_merge/day02
    val idmp: DataFrame = spark.read.parquet("user_profile/data/output/tag_merge/day02")
      .where(" tag_module='M000' ")
      .select("tag_value", "gid")
    idmp.printSchema()
    idmp.show(10, false)

    // 6、idmp的数据加工
    val hfileRdd: RDD[(ImmutableBytesWritable, KeyValue)] = idmp.rdd.map(row => {
      val id: String = row.getAs[String]("tag_value")
      val gid: Long = row.getAs[Long]("gid")
      val gidMd5: String = DigestUtils.md5Hex(gid + "").substring(0, 10) + date
      (id, gidMd5)
    }).sortByKey()
      .map(tp => (new ImmutableBytesWritable(Bytes.toBytes(tp._1)), new KeyValue(Bytes.toBytes(tp._1), "f".getBytes(), "q".getBytes(), Bytes.toBytes(tp._2))))

    // 6、存储成配置
    val outpath = "hdfs://hadoop11:9000/tmp/idx"
    hfileRdd.saveAsNewAPIHadoopFile(outpath,
      classOf[ImmutableBytesWritable],
      classOf[KeyValue],
      classOf[HFileOutputFormat2],
      job2.getConfiguration
    )

    // 7、关闭spark
    spark.close()
    println("索引表数据的HFile文件生成完毕...............................")


    println("准备导入索引表数据的HFile文件...............................")

    // 8、利用hbase提供的 LoadIncrementalHFiles.doBulkload() 来将Hfile导入hbase
    val tbl: TableName = TableName.valueOf("IDX_PROFILE_ID_GID")
    val conn: Connection = ConnectionFactory.createConnection(conf2)
    val admin: Admin = conn.getAdmin
    val table: Table = conn.getTable(tbl)
    val locator: RegionLocator = conn.getRegionLocator(tbl)
    val loader = new LoadIncrementalHFiles(conf2)
    loader.doBulkLoad(new Path(outpath), admin, table, locator)
    conn.close()
  }
}
