package cn.doitedu.profile.tagexport

import cn.doitedu.commons.utils.SparkUtil
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.client.{ConnectionFactory, TableDescriptorBuilder}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat2, TableOutputFormat}
import org.apache.hadoop.hbase.tool.LoadIncrementalHFiles
import org.apache.hadoop.hbase.{HBaseConfiguration, KeyValue, TableName}
import org.apache.hadoop.mapreduce.Job

/**
  * @date: 2019/8/10
  * @site: www.doitedu.cn
  * @author: hunter.d 涛哥
  * @qq: 657270652
  * @description: 将任意数据通过bulkloader导入hbase的模板程序
  *               测试之前，要在hbase中建表：
  *               hbase(main):017:0> create 'x','f'
  */
object Export2HbaseDemo {

  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "root")
    System.setProperty("spark.serializer", "org.apache.spark.serializer.KryoSerializer")


    val spark = SparkUtil.getSparkSession("")
    import spark.implicits._


    /**
      * 参数设置
      */
    val tableName = TableName.valueOf("x") // 目标表名
    val conf = HBaseConfiguration.create();

    //conf.set("hadoop.user.name", "root")
    val td = TableDescriptorBuilder.newBuilder(tableName).build()

    conf.set(TableOutputFormat.OUTPUT_TABLE, "x") // 指定输出的目标表
    conf.set("hbase.zookeeper.quorum", "doit01:2181,doit02:2181,doit03:2181")
    lazy val job = Job.getInstance(conf)

    job.setMapOutputKeyClass(classOf[ImmutableBytesWritable]) // 指定的其实就是rowkey类型
    job.setMapOutputValueClass(classOf[KeyValue])

    // 指定输出的outputformat使用的是HFileOutputFormat2，这样输出的结果文件类型就会变成HFile
    HFileOutputFormat2.configureIncrementalLoadMap(job, td)


    /**
      * 数据处理
      */
    val ds = spark.createDataset(Seq("b-2,100", "a-2,200", "c-2,300"))
    val rdd = ds.rdd.map(line => {
      val arr = line.split(",")
      val k = arr(0).getBytes()
      val v = arr(1).getBytes()
      val kv = new KeyValue(k, "f".getBytes(), "c".getBytes(), v)

      (new ImmutableBytesWritable(k), kv)
    }
    )
      .sortByKey()

    rdd.take(10).foreach(println)


    /**
      * 生成HFile
      */
    rdd.saveAsNewAPIHadoopFile("hdfs://doit01:8020/tmp/x5", classOf[ImmutableBytesWritable], classOf[KeyValue], classOf[HFileOutputFormat2], job.getConfiguration())


    println("HFile生成完毕，已经写入hdfs的目录  /tmp/x2 ")

    /**
      * 导入Hfile到hbase
      */
    val loadIncrementalHFiles = new LoadIncrementalHFiles(conf)
    val conn = ConnectionFactory.createConnection(conf)
    val table = conn.getTable(tableName)
    val admin = conn.getAdmin
    val regionLocator = conn.getRegionLocator(tableName)

    loadIncrementalHFiles.doBulkLoad(new Path("hdfs://doit01:8020/tmp/x5"), admin, table, regionLocator)


    spark.close()
  }

}
