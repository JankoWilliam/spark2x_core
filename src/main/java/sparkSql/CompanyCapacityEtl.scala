package sparkSql

import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.util.matching.Regex

object CompanyCapacityEtl {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("CompanyCapacityEtl")
//      .master("local")
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._

    //  获取日期分区参数
    require(!(args==null||args.length==0||args(0)==""),"Required PARTITION args")
    val pattern = new Regex("\\d{8}")
    val dateSome = pattern findFirstIn args(0)
    require(dateSome.isDefined,s"Required PARTITION args like 'yyyymmdd' but find ${args(0)}")
    val ptt_day = dateSome.get// 实际使用yyyymmdd格式日期
    println("update ptt_day : "+ptt_day)

    // 1.待提取公司：公司名称、信用代码
    spark.sql("SELECT company_name,honor_code FROM ci_cdm.dwd_company_data_f").createOrReplaceTempView("company")
    // 能力标签表
    spark.sql(s" select businnes_key_words  from ci_ods.ods_spider_qcc_company_product_remarks where ptt_day = '${ptt_day}'")
      .flatMap( row => (if (row(0)==null || row(0) == "") "null" else row(0).toString).split(","))
      .map(_.replace("\n","").trim)
      .distinct()
      .filter(row => row.length > 0)
      .toDF("name").createOrReplaceTempView("t")
    spark.sql("select ROW_NUMBER() OVER (ORDER BY name)  as id , name from t").createOrReplaceTempView("a")

    // 公司-标签表
    spark.sql("SELECT a.honor_code,a.businnes_key_words FROM " +
      s" (select honor_code,businnes_key_words  from ci_ods.ods_spider_qcc_company_product_remarks where ptt_day = '${ptt_day}') a " +
      "  LEFT SEMI JOIN company b ON a.honor_code = b.honor_code ")
      .flatMap( row => (if (row(1)==null || row(1) == "") "null" else row(1).toString.replace("\n","").trim).split(",").map(v => (row(0).toString,v.trim)))
      .toDF("honor_code","businnes_key_word")
      .filter(row => row(0)!=null && row(0).toString.trim.length > 0 && row(1)!=null && row(1).toString.length > 0)
      .createOrReplaceTempView("b")

    spark.sql("select id,name," +
      " 'bd_insert' as op_latest," +
      " 'bd_insert-大数据首次插入' as remark," +
      " 'bd' as create_user," +
      " unix_timestamp() as create_time," +
      " 'bd' as update_user," +
      " '0' as update_time," +
      " '0' as is_deleted from a ")
      .write
      .mode(SaveMode.Overwrite)
      .format("Hive")
      .saveAsTable("ci_ads.ads_qx_applet_company_capacity")

    spark.sql("select " +
      " b.honor_code as company_id," +
      " a.id as capacity_id," +
      " 'bd_insert' as op_latest," +
      " 'bd_insert-大数据首次插入' as remark," +
      " 'bd' as create_user," +
      " unix_timestamp() as create_time," +
      " 'bd' as update_user," +
      " '0' as update_time," +
      " '0' as is_deleted" +
      " from b LEFT JOIN a ON b.businnes_key_word = a.name")
      .write
      .mode(SaveMode.Overwrite)
      .format("Hive")
      .saveAsTable("ci_ads.ads_qx_applet_company_capacity_map")

    spark.stop()

  }
}
