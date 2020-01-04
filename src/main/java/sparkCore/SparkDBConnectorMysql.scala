package sparkCore

import info.xiaohei.spark.connector.mysql.MysqlConf
import org.apache.spark.{SparkConf, SparkContext}

object SparkDBConnectorMysql {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()
      .setAppName("test")
      .setMaster("local")
      .set("spark.mysql.host", "localhost")
      .set("spark.mysql.username", "root")
      .set("spark.mysql.password", "123456")
      .set("spark.mysql.port", "3306")
      .set("spark.mysql.db", "baidu_address")

    val sc = new SparkContext(sparkConf)

    implicit val mysqlConf = MysqlConf.createFromSpark(sc)
    import info.xiaohei.spark.connector.mysql._

    val res = sc.fromMysql[(Int,String,Int)]("company_address_acquired_baidu")
      .select("id","company_name","use_address")
      .where("  id = 10 ")
      .get

    res.foreach(println)

  }
}
