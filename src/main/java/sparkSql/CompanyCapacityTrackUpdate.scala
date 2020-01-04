package sparkSql

import java.sql.DriverManager
import java.util.Properties

import org.apache.spark.sql.{SaveMode, SparkSession}

object CompanyCapacityTrackUpdate {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("CompanyCapacityTrackUpdate")
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

    val conn = DriverManager.getConnection(url,properties.getProperty("user"),properties.getProperty("password"))
//    try {
      val pstm1 = conn.prepareStatement("truncate qx_applet_company_capacity")
      val pstm2 = conn.prepareStatement("truncate qx_applet_company_capacity_map")
      val pstm3 = conn.prepareStatement("truncate qx_applet_company_track")
      val pstm4 = conn.prepareStatement("truncate qx_applet_company_track_map")
      pstm1.execute()
      pstm2.execute()
      pstm3.execute()
      pstm4.execute()
      if(pstm1 != null){
        pstm1.close()
      }
      if(pstm2 != null){
        pstm2.close()
      }
      if(pstm3 != null){
        pstm3.close()
      }
      if(pstm4 != null){
        pstm4.close()
      }
      spark.sql("SELECT * FROM ci_ads.ads_qx_applet_company_capacity")
        .write.mode(SaveMode.Append).jdbc(url,"qx_applet_company_capacity",properties)
      spark.sql("SELECT * FROM ci_ads.ads_qx_applet_company_capacity_map")
        .write.mode(SaveMode.Append).jdbc(url,"qx_applet_company_capacity_map",properties)
      spark.sql("SELECT * FROM ci_ads.ads_qx_applet_company_track")
        .write.mode(SaveMode.Append).jdbc(url,"qx_applet_company_track",properties)
      spark.sql("SELECT * FROM ci_ads.ads_qx_applet_company_track_map")
        .write.mode(SaveMode.Append).jdbc(url,"qx_applet_company_track_map",properties)
//    } catch {
//      case e : Exception =>{
//        println(e)//conn.rollback()
//      }
//    } finally {
      if(conn != null){
        conn.close()
      }
//    }

  }
}
