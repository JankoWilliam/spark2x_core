package hbaseTest.saltTets

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConversions._

object HbaseSaltTest {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder
      .appName("HBase")
      .master("local[*]")
      .getOrCreate()

    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", "172.16.20.130:2181,172.16.20.131:2181,172.16.20.132:2181")
    conf.set(TableInputFormat.INPUT_TABLE, "salting_test")
    conf.set(TableInputFormat.SCAN_ROW_START, "A-6991-1572523994712")
    conf.set(TableInputFormat.SCAN_ROW_STOP, "A-6992-1572523988678 ")

    val HBaseRdd = sparkSession.sparkContext.newAPIHadoopRDD(conf, classOf[SaltRangeTableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result])

    HBaseRdd.take(10).foreach(println)
    println("===============================================")
    HBaseRdd.foreach {
      case (_, result) =>
      val rowKey = Bytes.toString(result.getRow)
      val cell = result.listCells()
      cell.foreach { item =>
        val family = Bytes.toString(item.getFamilyArray, item.getFamilyOffset, item.getFamilyLength)
        val qualifier = Bytes.toString(item.getQualifierArray,
          item.getQualifierOffset, item.getQualifierLength)
        val value = Bytes.toString(item.getValueArray, item.getValueOffset, item.getValueLength)
        println(rowKey + " \t " + "column=" + family + ":" + qualifier + ", " +
          "timestamp=" + item.getTimestamp + ", value=" + value)
      }
    }
  }
}