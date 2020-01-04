package sparkSql

import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.mutable.ListBuffer
import scala.util.matching.Regex

object CompanyTrackEtl {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("CompanyTrackEtl")
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
//    val ptt_day = "20191209"

    // 1.待提取公司：公司名称、信用代码
    spark.sql("SELECT company_name,honor_code FROM ci_cdm.dwd_company_data_f").createOrReplaceTempView("company")
    // 2.业务赛道标签划分标准 文件
    val remarks_category = spark.read.textFile("/user/clfuzhanpeng/remarks_category.txt").map(v => {
      val remarks = v.split("\t")
      (remarks(0),remarks)
    }).collect()

    /**
      * 3.cdm_company_remarks_category
      */
    spark.sql("SELECT a.honor_code,a.businnes_key_words FROM " +
      s" (SELECT honor_code,businnes_key_words  FROM ci_ods.ods_spider_qcc_company_product_remarks where ptt_day = '${ptt_day}') a ")
      .flatMap( row => (if (row(1)==null || row(1) == "") "null" else row(1).toString).split(",").map((row(0).toString,_)))
      .rdd
      .groupByKey()
      .flatMap(company_key_words => {
        val category = ListBuffer[String]()
        company_key_words._2.map(v1 => {
          remarks_category.map(v2 =>{
            if (v2._2.contains(v1)) category.+=(v2._1)
          })
        })
        (if (category.isEmpty) ListBuffer("其他") else category).distinct.map((company_key_words._1,_))
      }).toDF("honor_code","remarks_category")
      .createOrReplaceTempView("a")

    spark.sql("SELECT " +
      " ROW_NUMBER() OVER (ORDER BY name)  as id," +
      " 0 as parent_id," +
      " name," +
      " 'bd_insert' as op_latest," +
      " 'bd_insert-大数据首次插入' as remark," +
      " 'bd' as create_user," +
      " unix_timestamp() as create_time," +
      " 'bd' as update_user," +
      " '0' as update_time," +
      " '0' as is_deleted " +
      " FROM (SELECT distinct(remarks_category) as name FROM a) t")
      .createOrReplaceTempView("b")


    spark.sql("SELECT * FROM b")
      .write.mode(SaveMode.Overwrite).saveAsTable("ci_ads.ads_qx_applet_company_track")

    spark.sql("SELECT " +
      " t.honor_code as company_id," +
      " b.id as track_id," +
      " 'bd_insert' as op_latest," +
      " 'bd_insert-大数据首次插入' as remark," +
      " 'bd' as create_user," +
      " unix_timestamp() as create_time," +
      " 'bd' as update_user," +
      " '0' as update_time," +
      " '0' as is_deleted " +
      " FROM (select a.honor_code,a.remarks_category from a LEFT SEMI JOIN company b ON a.honor_code = b.honor_code) t" +
      " LEFT JOIN b ON t.remarks_category = b.name")
      .write.mode(SaveMode.Overwrite).saveAsTable("ci_ads.ads_qx_applet_company_track_map")

  }
}
