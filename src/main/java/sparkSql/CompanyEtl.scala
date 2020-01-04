package sparkSql

import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.util.matching.Regex

/**
  * 企业基础信息表清洗
  */
object CompanyEtl {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("CompanyEtl")
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
    // 2.工商信息：信用编码、经营状态、所属行业
    spark.sql("SELECT a.honor_code,a.business_status,a.field FROM " +
      " (SELECT honor_code,business_status,field FROM ci_ods.ods_spider_qcc_company_base_register " +
      s" WHERE ptt_day = '${ptt_day}') a " +
      " LEFT SEMI JOIN company b ON a.honor_code = b.honor_code ")
        .createOrReplaceTempView("a")
    // 3.公司logo：
    spark.sql("SELECT a.company_name,a.honor_code,concat('https://253-assets.oss-cn-hangzhou.aliyuncs.com/',a.oss_path) as oss_path FROM " +
      " (SELECT * FROM ci_ods.ods_spider_qcc_company_media " +
      s" WHERE ptt_day = '${ptt_day}' AND pos = '1') a " +
      "  LEFT SEMI JOIN company b ON a.honor_code = b.honor_code ")
        .createOrReplaceTempView("b")
    // 4.1 company_common_statistics:信用编码、号码、email、公司简介、网站、tags
    spark.sql("SELECT a.honor_code,a.company_name," +
      " CASE WHEN length(a.telphone) > 1 THEN a.telphone ELSE '-' END AS telphone," +
      " CASE WHEN length(a.email) > 1 THEN a.email ELSE '-' END AS email," +
      " CASE WHEN length(a.site) > 1 THEN a.site ELSE '-' END AS site," +
      " a.introduce,a.tags FROM " +
      " (SELECT honor_code,telphone,email,introduce,site,company_name,tags FROM ci_ods.ods_spider_qcc_company_common_statistics " +
      s" WHERE ptt_day = '${ptt_day}') a " +
      " LEFT SEMI JOIN company b ON a.honor_code = b.honor_code ")
        .createOrReplaceTempView("c")
    // 4.2  qx_applet_company_etl.jar(启信宝): 号码、email
    spark.sql("SELECT a.honor_code,a.company_name," +
      " CASE WHEN a.phone IS NOT NULL and length(a.phone) > 4 THEN a.phone  ELSE '-' END AS phone ," +
      " CASE WHEN a.email IS NOT NULL and length(a.email) > 4 THEN a.email ELSE '-' END AS email" +
      " FROM " +
      " (SELECT honor_code,phone,email,company_name FROM ci_ods.ods_spider_qixinbao_company_contact " +
      s" WHERE ptt_day = '20191212') a " +
      " LEFT SEMI JOIN company b ON a.company_name = b.company_name ")
        .createOrReplaceTempView("cc")

    // 5.1 招聘信息：公司简介
    spark.sql("SELECT a.company_name,a.company_intro FROM " +
      "(SELECT company_name,company_intro FROM " +
      "(SELECT company_name,company_intro,row_number() OVER(PARTITION BY company_name ORDER BY LENGTH(company_intro) desc) as row_num FROM " +
      s"(SELECT company_name,company_intro FROM ci_ods.ods_spider_recruit_boss_content WHERE ptt_day ='${ptt_day}' UNION " +
      s"SELECT company_name,company_intro FROM ci_ods.ods_spider_recruit_lg_content WHERE ptt_day ='${ptt_day}' UNION " +
      s"SELECT companyname as company_name,company_intro FROM ci_ods.ods_spider_recruit_zhilian_job WHERE ptt_day = '${ptt_day}') t) tt WHERE tt.row_num = 1) a " +
      "LEFT SEMI JOIN company b ON a.company_name = b.company_name ")
        .createOrReplaceTempView("d")
    // 5.2 结合上述两个公司简介，工商信息公司简介为空时取招聘信息公司简介
    spark.sql("SELECT c.honor_code,c.company_name," +
      " CASE WHEN (c.introduce IS NOT NULL AND length(c.introduce) > 5) THEN c.introduce " +
      " WHEN (d.company_intro IS NOT NULL AND length(d.company_intro) > 5) THEN d.company_intro " +
      " ELSE '-' END AS description " +
      " FROM c LEFT JOIN d ON c.company_name = d.company_name")
        .createOrReplaceTempView("dd")
    // 6.tags(status)
    spark.sql("SELECT honor_code,tags FROM c")
      .map(row => {
        (row(0).toString,extractTags(if (row(1)==null) "" else row(1).toString))
      }).toDF("honor_code","newtags")
      .createOrReplaceTempView("e")
    // 7.知识产权统计字段表：专利数、商标数、著作权数、icp数量
    spark.sql("SELECT a.honor_code,a.patent_total,a.brand_total,a.works_total,a.website_total FROM " +
      " (SELECT honor_code,patent_total,brand_total,works_total,website_total FROM ci_ods.ods_spider_qcc_company_property_statistics " +
      s" WHERE ptt_day = '${ptt_day}') a " +
      " LEFT SEMI JOIN company b ON a.honor_code = b.honor_code ")
        .createOrReplaceTempView("f")
    // 8.百度地址表：地址，经度，纬度,及地址（取获取经纬度的地址作为公司地址）
    spark.sql("SELECT a.company_name," +
      " CASE WHEN length(a.use_address) > 1 THEN a.use_address ELSE '-' END AS use_address," +
      " a.location_lng,a.location_lat FROM " +
      " (SELECT company_name,use_address,location_lng,location_lat FROM ci_cdm.cdm_company_address_acquired_baidu) a " +
      " LEFT SEMI JOIN company b ON a.company_name = b.company_name ")
        .createOrReplaceTempView("g")
    // 9.分类

    // 10.高德地址表：地址，经度，纬度
    spark.sql("SELECT a.company_name,a.use_address,a.location_lng,a.location_lat FROM " +
      " (SELECT company_name,use_address,location_lng,location_lat FROM ci_cdm.cdm_company_address_acquired_gaode) a " +
      " LEFT SEMI JOIN company b ON a.company_name = b.company_name ")
      .createOrReplaceTempView("i")

    val nafillMap = Map("address" -> "","bd_longitude" -> 0.0,"bd_latitude" -> 0.0,"gd_longitude" -> 0.0,"gd_latitude" -> 0.0)

    spark.sql("SELECT " +
      " t.honor_code AS id," +
      " t.company_name AS name," +
      " b.oss_path AS logo," +
      " substr(dd.description,1,1000) AS description," +
      " CASE WHEN cc.phone IS NOT NULL and length(cc.phone) > 4 THEN cc.phone  ELSE '-' END  AS phone," +
      " CASE WHEN cc.email IS NOT NULL and length(cc.email) > 4 THEN cc.email ELSE '-' END AS mail," +
      " c.site AS website," +
      " a.field AS industry," +
      " g.use_address AS address," +
      " '' AS type," +
      " e.newtags AS status," +
      " cast(f.patent_total as INTEGER) AS knowledge_zl_num," +
      " cast(f.brand_total as INTEGER) AS knowledge_sb_num," +
      " cast(f.works_total as INTEGER) AS knowledge_zzq_num," +
      " cast(f.website_total as INTEGER) AS knowledge_icp_num," +
      " cast(g.location_lng as DOUBLE) AS bd_longitude," +
      " cast(g.location_lat as DOUBLE) AS bd_latitude," +
      " cast(i.location_lng as DOUBLE) AS gd_longitude," +
      " cast(i.location_lat as DOUBLE) AS gd_latitude," +
      " 'bd_insert' AS op_latest," +
      " 'bd_insert-大数据首次插入' AS remark," +
      " 'bd' AS create_user," +
      " unix_timestamp() AS create_time," +
      " 'bd' AS update_user," +
      " 0 AS update_time," +
      " 0 AS is_deleted " +
      " FROM company t " +
      " LEFT JOIN a ON t.honor_code = a.honor_code " +
      " LEFT JOIN b ON t.company_name = b.company_name " +
      " LEFT JOIN c ON t.honor_code = c.honor_code " +
      " LEFT JOIN cc ON t.honor_code = cc.honor_code " +
      " LEFT JOIN dd ON t.honor_code = dd.honor_code " +
      " LEFT JOIN e ON t.honor_code = e.honor_code " +
      " LEFT JOIN f ON t.honor_code = f.honor_code " +
      " LEFT JOIN g ON t.company_name = g.company_name " +
      " LEFT JOIN i ON t.company_name = i.company_name "
    ).na.fill(nafillMap).write.mode(SaveMode.Overwrite).saveAsTable("ci_ads.ads_qx_applet_company")



  }

  /**
    * 提取指定公司标签属性，按，分隔
    * @param tags 公司原有属性标签
    * @return  按照指定规则提取的公司属性标签
    */
  def extractTags(tags:String):String={
    val keyTagList = List("存续", "在业", "经营异常", "注销",
      "高新技术企业", "吊销", "严重违法", "有失信被执行人",
      "天使轮", "新四板", "A轮", "并购", "B轮", "Pre-A轮",
      "新三板", "种子轮", "C轮", "A+轮",
      "已退市(新三板)", "B+轮", "D轮",
      "E轮及以后", "C+轮", "Pre-B轮", "D+轮")
    return tags.split("\\|").toList.filter(x =>keyTagList.contains(x)).mkString(",")
  }
}
