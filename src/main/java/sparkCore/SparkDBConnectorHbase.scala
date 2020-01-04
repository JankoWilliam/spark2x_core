package sparkCore

import info.xiaohei.spark.connector.hbase.builder.reader.HBaseReaderBuilder
import info.xiaohei.spark.connector.hbase.transformer.reader.{CustomDataReader, DataReader}
import org.apache.spark.{SparkConf, SparkContext}

object SparkDBConnectorHbase {
  case class MyClass(age: Int, uuid: String)

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("test")
    sparkConf.set("spark.hbase.host", "172.16.20.130:2181,172.16.20.131:2181,172.16.22.130:2181")
    val sc = new SparkContext(sparkConf)

    import info.xiaohei.spark.connector.hbase._

    val hbaseRdd = sc.fromHBase[(String, String, String)]("salting_test")
      .select("age", "uuid")
      .inColumnFamily("f")
      .withStartRow("A-6991")
      .withEndRow("A-6993")
    //当rowkey中有随机的salt前缀时,将salt数组传入即可自动解析
    //得到的rowkey将会是原始的,不带salt前缀的
    //      .withSalt(saltArray)
    hbaseRdd.foreach(println)


    implicit def myReaderConversion: DataReader[MyClass] = new CustomDataReader[(Int, String), MyClass] {
      override def convert(data: (Int , String)): MyClass = MyClass(data._1, data._2)
    }


    val classRdd = sc.fromHBase[MyClass]("salting_test")
      .select("name","age")
      .inColumnFamily("f")
      .withStartRow("A-6991")
      .withEndRow("A-6993").map(c => (c.age,c.uuid))
  }
}
