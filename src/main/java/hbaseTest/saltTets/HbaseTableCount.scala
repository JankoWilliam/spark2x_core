package hbaseTest.saltTets

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.client.Result
import org.apache.spark.sql.SparkSession

object HbaseTableCount {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder
      .appName("HBase")
      .master("local[*]")
      .getOrCreate()

    val conf = HBaseConfiguration.create()
//    conf.set("hbase.zookeeper.quorum", "172.16.20.130:2181,172.16.20.131:2181,172.16.20.132:2181")
    //    conf.set(TableInputFormat.INPUT_TABLE, "active_mobile_label_list")

    conf.set("hbase.zookeeper.quorum", "hb-proxy-pub-j6cd99qiw2s30322y-master1-001.hbase.rds.aliyuncs.com:2181,hb-proxy-pub-j6cd99qiw2s30322y-master2-001.hbase.rds.aliyuncs.com:2181,hb-proxy-pub-j6cd99qiw2s30322y-master3-001.hbase.rds.aliyuncs.com:2181")
    conf.set(TableInputFormat.INPUT_TABLE, "ALIHBASE_KH_NUMBER")

    val HBaseRdd = sparkSession.sparkContext.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result])

    println(HBaseRdd.count())
  }
}
