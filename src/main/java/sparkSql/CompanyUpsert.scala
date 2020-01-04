  package sparkSql

import java.sql.DriverManager
import java.util.{Date, Properties}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import scala.util.control.Breaks

object CompanyUpsert {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("CompanyRemarksNum")
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._

    Class.forName("com.mysql.jdbc.Driver")
    val url = "jdbc:mysql://172.16.20.53:53306/bigdata"
    val driver = "com.mysql.jdbc.Driver"
    val properties = new Properties()
    properties.put("user", "bigdata")
    properties.put("password", "Bigdata@253.con.Bd")
    properties.put("driver", driver)
    //
    var destination_honor = spark.read.jdbc(url,"(select id from qx_applet_company) t",properties).map(_.getString(0)).collect()
    val sourceData = spark.sql("SELECT * FROM ci_ads.ads_qx_applet_company").na.fill("")
    val columns: Array[String] = sourceData.columns

    sourceData.foreachPartition(batch => {
      val conn = DriverManager.getConnection(url,properties.getProperty("user"),properties.getProperty("password"))
//      try {
        batch.foreach(row => {
          var pstm = conn.prepareStatement("insert into qx_applet_company(" + columns.reduce(_+","+_) + ") values(" +
            "?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?" +
            ")")
          var honor = row.getString(0)

          if (useLoop(destination_honor,honor)) {
            pstm = conn.prepareStatement("update qx_applet_company set " + columns.reduce(_+"=?,"+_) + "=? where id = ?")
            pstm.setString(20,"bd_update")
            pstm.setString(21,"bd_update-大数据更新")
            pstm.setLong(25,new Date().getTime)//更新时间
            pstm.setString(27,row.getString(0))//honor
          } else {
            pstm.setString(20,"bd_insert")
            pstm.setString(21,"bd_insert-大数据首次插入")
            pstm.setLong(25,0)//更新时间
          }
          pstm.setString(1,if (row(0) == null) "-" else row.getString(0))
          pstm.setString(2,if (row(1) == null) "-" else row.getString(1))
          pstm.setString(3,if (row(2) == null) "-" else row.getString(2))
          pstm.setString(4,if (row(3) == null) "-" else row.getString(3))
          pstm.setString(5,if (row(4) == null) "-" else row.getString(4))
          pstm.setString(6,if (row(5) == null) "-" else row.getString(5))
          pstm.setString(7,if (row(6) == null) "-" else row.getString(6))
          pstm.setString(8,if (row(7) == null) "-" else row.getString(7))
          pstm.setString(9,if (row(8) == null) "-" else row.getString(8))
          pstm.setString(10,if (row(9) == null) "-" else row.getString(9))
          pstm.setString(11,if (row(10) == null) "-" else row.getString(10))
          pstm.setInt(12,if (row(11) == null) 0 else row.getInt(11))
          pstm.setInt(13,if (row(12) == null) 0 else row.getInt(12))
          pstm.setInt(14,if (row(13) == null) 0 else row.getInt(13))
          pstm.setInt(15,if (row(14) == null) 0 else row.getInt(14))
          pstm.setDouble(16,if (row(15) == null) 0.0 else row.getDouble(15))
          pstm.setDouble(17,if (row(16) == null) 0.0 else row.getDouble(16))
          pstm.setDouble(18,if (row(17) == null) 0.0 else row.getDouble(17))
          pstm.setDouble(19,if (row(18) == null) 0.0 else row.getDouble(18))
          pstm.setString(22,if (row(21) == null) "-" else row.getString(21))
          pstm.setLong(23,if (row(22) == null) 0L else row.getLong(22))
          pstm.setString(24,if (row(23) == null) "-" else row.getString(23))
          pstm.setInt(26,if (row(25) == null) 0 else row.getInt(25))

          pstm.execute()
          if(pstm != null){
            pstm.close()
          }
        })
//      } catch {
//        case e:Exception => println(e)//conn.rollback()
//      } finally {
        if(conn != null){
          conn.close()
        }
//      }

    })


  }

  /**
    * 使用循环判断
    * @param arr 数组
    * @param target 目标值
    * @return  数组中是否包含目标值的结果
    */
  def useLoop(arr:Array[String],target:String):Boolean={
    var b = false
    val loop = new Breaks
    loop.breakable {
      for (i <- arr) {
        if (i.equals(target)) {
          println(i)
          b = true
          loop.break()
        }
      }
    }
    return b
  }
}
