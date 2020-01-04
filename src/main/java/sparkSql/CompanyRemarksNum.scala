package sparkSql

import java.util.Properties
import scala.collection.mutable.ListBuffer
import org.apache.spark.sql.{SaveMode, SparkSession}

object CompanyRemarksNum {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("CompanyRemarksNum")
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._

    spark.sql(" select company_name,businnes_key_words  from ci_ods.ods_spider_qcc_company_product_remarks ")
      .flatMap( row => (if (row(1)==null || row(1) == "") "null" else row(1).toString).split(",").map((_,row(0).toString)))
      .toDF("businnes_key_word","company_name")
      .groupBy("businnes_key_word")
      .count()
      .write
      .mode(SaveMode.Overwrite)
      .format("Hive")
      .saveAsTable("ci_cdm.cdm_businnes_key_word_count")

    val remarks_category = spark.read.textFile("/user/clfuzhanpeng/remarks_category.txt").map(v => {
      val remarks = v.split("\t")
      (remarks(0),remarks)
    }).collect()

    /**
      * cdm_company_remarks_category
      */
    spark.sql(" select company_name,businnes_key_words  from ci_ods.ods_spider_qcc_company_product_remarks where ptt_day = '20191129' ")
      .flatMap( row => (if (row(1)==null || row(1) == "") "null" else row(1).toString).split(",").map((row(0).toString,_)))
      .rdd
      .groupByKey()
      .map(company_key_words => {
        val category = ListBuffer[String]()
        company_key_words._2.map(v1 => {
          remarks_category.map(v2 =>{
            if (v2._2.contains(v1)) category.+=(v2._1)
          })
        })
        (company_key_words._1,if (category.isEmpty) "其他" else category.distinct.reduce((a,b)=>a+","+b))
      }).toDF("company_name","remarks_category")
        .write.mode(SaveMode.Overwrite).saveAsTable("ci_cdm.cdm_company_remarks_category")











    Class.forName("com.mysql.jdbc.Driver")
    val url_data = "jdbc:mysql://172.16.20.53:53306/bigdata"
    val driver_data = "com.mysql.jdbc.Driver"
    val properties_data = new Properties()
    properties_data.put("user", "bigdata")
    properties_data.put("password", "Bigdata@253.con.Bd")
    properties_data.put("driver", driver_data)

    spark.sql("select * from ci_cdm.cdm_product_software_adress").
      toDF("company_name","gs_address","zp_address")
      .write.mode(SaveMode.Overwrite).jdbc(url_data,"company_address_source",properties_data)

    spark.read.jdbc(url_data,"company_address_acquired_gaode",properties_data).write.mode(SaveMode.Overwrite).saveAsTable("ci_cdm.cdm_company_address_acquired_gaode")


  }
}
