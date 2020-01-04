package sparkSql

import org.apache.spark.sql.{ SaveMode, SparkSession}

/**
  * 产品&软著匹配招聘地址（boos、拉钩、智联，取最长的地址），结果入到ci_cdm
  */
object CompanyAddressEtl {

  def main(args: Array[String]): Unit = {


    val spark = SparkSession.builder()
      .appName("company_address_etl")
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._
    // 拉钩公司和地址清洗
    val lgCompanyAddress = spark.sql("select company_name,company_place from ci_ods.ods_spider_recruit_lg_content")
      .map(row => {
        val row_1 = if (row(1)==null) "" else row(1).toString
        val address = row_1.split(",")
          .flatMap(_.split("\\|"))
          .flatMap(_.split("，"))
          .maxBy(_.length)
        (row(0).toString,address)
      }).rdd.groupByKey().map(v => (v._1,v._2.maxBy(_.length))).toDF("company_name","company_address")
    // 智联公司和地址清洗
    val zlCompanyAddress = spark.sql("SELECT companyname,company_address FROM   (SELECT companyname,company_address,row_number() OVER(PARTITION BY companyname ORDER BY length(company_address) DESC) row_num    FROM ci_ods.ods_spider_recruit_zhilian_job) t WHERE row_num = 1")
    // boss公司和地址清洗
    val bossCompanyAddress = spark.sql("SELECT company_name,company_location from ci_ods.ods_spider_recruit_bosszp")
      .map(row => {
        val row_1 = if (row(1)==null) "" else row(1).toString
        val address = row_1.split(";")
          .maxBy(_.length)
        (row(0).toString,address)
      } ).toDF("company_name","company_address")
    // 存在产品和软著公司和工商地址清洗
    val productSoftwareCommon = spark.sql("SELECT a.company_name, b.address from ( SELECT DISTINCT company_name  FROM    (SELECT company_name     FROM ci_ods.ods_spider_qcc_later_software_company     UNION SELECT company_name     FROM ci_ods.ods_spider_qcc_company_product_remarks) t) a     JOIN ci_ods.ods_spider_qcc_company_base_register b     ON a.company_name = b.company_name")
    productSoftwareCommon.createOrReplaceTempView("a")
    // boss、拉钩、智联地址取最长的
    lgCompanyAddress.union(zlCompanyAddress).union(bossCompanyAddress)
      .filter(row => row(0)!=null && !row(0).toString.isEmpty && row(0)!="None" &&  row(1)!=null)
      .map(row => (row.getString(0),row.getString(1)))
      .rdd.groupByKey().map(v => (v._1,v._2.maxBy(_.length))).toDF("company_name","company_address_zp")
      .createOrReplaceTempView("b")
    // 公司，工商地址，招聘地址  数据入到ci_cdm
    spark.sql("select a.company_name,a.address,b.company_address_zp from a left join b on a.company_name = b.company_name")
        .write
        .mode(SaveMode.Overwrite)
        .format("Hive")
        .saveAsTable("ci_cdm.cdm_product_software_adress")

    spark.stop()

  }


}
