package sparkSql

import java.text.SimpleDateFormat
import java.util.Date
import org.apache.spark.sql.{Row, SaveMode, SparkSession}

import scala.util.matching.Regex

object CompanyJudgeScoring {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("CompanyJudgeScoring")
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._

    spark.sql(
      """
        |SELECT c.company_name
        |FROM
        |  (SELECT DISTINCT a.company_name
        |   FROM ci_ods.ods_spider_qcc_company_product_remarks a
        |   JOIN
        |     (SELECT DISTINCT company_name
        |      FROM ci_ods.ods_spider_qcc_company_base_register
        |      WHERE ptt_day='20191129'
        |        AND business_status NOT LIKE '%销%') b ON a.company_name=b.company_name
        |   WHERE a.ptt_day='20191129') c
        |LEFT JOIN
        |  (SELECT DISTINCT company_name
        |   FROM cl_ods.ods_erp_tek_customer_df) d ON c.company_name=d.company_name
        |WHERE d.company_name IS NULL
      """.stripMargin).createOrReplaceTempView("company")

    val sourceData = spark.sql(
      """
        |SELECT a.company_name,
        |       b.honor_code,
        |       b.reg_capital,
        |       b.scale_count,
        |       b.establish_date,
        |       CASE WHEN tags LIKE '%高新技术企业%' THEN 1 ELSE 0 END AS tag_gaoxin,
        |       prod_name_num,
        |       website_total,
        |       works_total,
        |       software_total,
        |       patent_total,
        |       brand_total,
        |       finance_total,
        |       CASE WHEN g.company_name IS NOT NULL THEN 1 ELSE 0 END AS has_recruit
        |FROM company a
        |JOIN
        |  (SELECT *
        |   FROM ci_ods.ods_spider_qcc_company_base_register
        |   WHERE ptt_day = '20191210') b ON a.company_name = b.company_name
        |LEFT JOIN
        |  (SELECT company_name,
        |          tags
        |   FROM ci_ods.ods_spider_qcc_company_common_statistics
        |   WHERE ptt_day = '20191210' ) c ON a.company_name = c.company_name
        |LEFT JOIN
        |  (SELECT *
        |   FROM ci_ods.ods_spider_qcc_company_property_statistics
        |   WHERE ptt_day = '20191210') d ON a.company_name = d.company_name
        |LEFT JOIN
        |  (SELECT *
        |   FROM ci_ods.ods_spider_qcc_company_run_statistics
        |   WHERE ptt_day = '20191210') e ON a.company_name = e.company_name
        |LEFT JOIN
        |  (SELECT company_name,
        |          count(prod_name) prod_name_num
        |   FROM ci_ods.ods_spider_qcc_company_product_main
        |   WHERE ptt_day = '20191210'
        |   GROUP BY company_name) f ON a.company_name = f.company_name
        |LEFT JOIN
        |  (SELECT DISTINCT company_name
        |   FROM
        |     (SELECT company_name
        |      FROM ci_ods.ods_spider_recruit_boss_content
        |      WHERE ptt_day = '20191210'
        |      UNION SELECT company_name
        |      FROM ci_ods.ods_spider_recruit_lg_content
        |      UNION SELECT companyname AS company_name
        |      FROM ci_ods.ods_spider_recruit_zhilian_job) t) g ON a.company_name = g.company_name
      """.stripMargin).cache()

    sourceData.map(row => {
      (row.getString(0),
        row.getString(1),
        if (row(2) == null) "" else row.getString(2), reg_capital_judge(row(2)),
        if (row(3) == null) "" else row.getString(3), employeeNumJudge(row(3)),
        if (row(4) == null) "" else row.getString(4), yearJudge(row(4)),
        if (row(5) == null) 0 else row.getInt(5), boolJudge(row(5), (0, 12)),
        if (row(6) == null) 0 else row.getLong(6), boolJudge(row(6), (0, 8)),
        if (row(7) == null) "" else row.getString(7), numJudge(row(7))
      )
    })
      .toDF("company_name", "honor_code", "reg_capital", "reg_capital_score", "scale_count", "scale_count_score", "establish_date", "establish_date_score",
        "tag_gaoxin", "tag_gaoxin_score", "prod_name_num", "prod_name_num_score", "website_total", "website_total_score")
      .createOrReplaceTempView("a")

    sourceData.map(row => {
      (row.getString(0),
        row.getString(1),
        if (row(8) == null) "" else row.getString(8), boolJudge(row(8), (0, 8)),
        if (row(9) == null) "" else row.getString(9), boolJudge(row(9), (0, 8)),
        if (row(10) == null) "" else row.getString(10), boolJudge(row(10), (0, 8)),
        if (row(11) == null) "" else row.getString(11), boolJudge(row(11), (0, 4)),
        if (row(12) == null) "" else row.getString(12), boolJudge(row(12), (0, 12)),
        if (row(13) == null) 0 else row.getInt(13), boolJudge(row(13), (0, 12))
      )
    })
      .toDF("company_name", "honor_code", "works_total", "works_total_score", "software_total", "software_total_score", "patent_total", "patent_total_score",
        "brand_total", "brand_total_score", "finance_total", "finance_total_score", "has_recruit", "has_recruit_score")
      .createOrReplaceTempView("b")

    spark.sql("SELECT a.*,works_total,works_total_score,software_total,software_total_score,patent_total,patent_total_score," +
      " brand_total,brand_total_score,finance_total,finance_total_score,has_recruit,has_recruit_score," +
      " reg_capital_score+scale_count_score+establish_date_score+tag_gaoxin_score+prod_name_num_score+website_total_score+" +
      " works_total_score+software_total_score+patent_total_score+brand_total_score+finance_total_score+has_recruit_score AS total_score FROM a " +
      " LEFT JOIN b ON a.company_name = b.company_name").write.mode(SaveMode.Overwrite).saveAsTable("ci_cdm.cdm_company_judgescoring")


  }

  // 注册资本评分
  def reg_capital_judge(reg_capital: Any): Double = {
    if (reg_capital == null || reg_capital.toString == "") {
      0.0
    } else {
      val str = reg_capital.toString
      val pattern = new Regex("\\d+")
      val dataSome = pattern findFirstIn str
      if (dataSome.isEmpty) {
        0.0
      } else {
        val ranges: List[(String, Double)] = List(("0-100", 2.4), ("100-500", 4.8), ("500-1000", 7.2), ("1000-10000", 9.6), ("10000-1000000", 12))
        val unit = new Regex("\\D+").findFirstMatchIn(str).getOrElse("")
        val tuples: List[(String, Double)] = List(
          ("元人民币", 0.001), ("万加元", 5.3382), ("万日元", 0.06474), ("万欧元", 7.8305),
          ("万澳大利亚元", 4.8355), ("万德国马克", 12.0571), ("万美元", 7.0304), ("万港元", 0.9007),
          ("万新加坡元", 5.1843), ("万瑞士法郎", 7.0052), ("万元人民币", 1.0), ("万瑞典克朗", 0.7492),
          ("万澳门元", 0.8744), ("万丹麦克朗", 1.048), ("万英镑", 9.2886))
        val result = dataSome.getOrElse(0).toString.toInt * tuples.find(_._1.equals(unit.toString)).getOrElse(("", 1.0))._2.toDouble
        rangeJudge(result, ranges)
      }
    }
  }

  // 范围评分
  def rangeJudge(num: Double, rangeMark: List[(String, Double)]): Double = {
    if (num == 0) {
      0.0
    } else {
      rangeMark.find(v => {
        val minMax = v._1.split("-")
        if (minMax.length == 2) {
          num > minMax(0).toDouble && num <= minMax(1).toDouble
        } else {
          false
        }
      }).getOrElse(("", 0.0))._2.toDouble
    }
  }

  // 注册年限评分
  def yearJudge(establish_date: Any): Double = {
    if (establish_date == null || establish_date.toString == "") {
      0.0
    } else {
      val str = establish_date.toString
      val pattern = new Regex("\\d{4}-\\d{2}-\\d{2}")
      val dataSome = pattern findFirstIn str
      if (dataSome.isEmpty) {
        0.0
      } else {
        val parse = new SimpleDateFormat("yyyy-MM-dd")
        val year = (parse.parse(parse.format(new Date)).getTime - parse.parse(dataSome.getOrElse("9999-99-99").toString).getTime).toDouble / 1000 / 3600 / 24 / 365
        val ranges: List[(String, Double)] = List(("0-3", 0.8), ("3-5", 1.6), ("5-10", 2.4), ("10-20", 3.2), ("20-1000000", 4))
        rangeJudge(if (year > 0) year else 0.0, ranges)
      }
    }
  }

  // 数量评分
  def numJudge(num: Any): Double = {
    if (num == null || num.toString == "") {
      0.0
    } else {
      val str = num.toString
      val pattern = new Regex("\\d+")
      val dataSome = pattern findFirstIn str
      if (dataSome.isEmpty) {
        0.0
      } else {
        val ranges: List[(String, Double)] = List(("0-5", 1.6), ("5-10", 3.2), ("10-20", 4.8), ("20-50", 6.4), ("50-1000000", 8))
        val result = dataSome.getOrElse("0.0").toDouble
        rangeJudge(result, ranges)
      }
    }
  }

  // 员工数量评分
  def employeeNumJudge(employeeNum: Any): Double = {
    if (employeeNum == null || employeeNum.toString == "") {
      0.0
    } else if(employeeNum.equals("少于50人")){
      0.8
    } else {
      val str = employeeNum.toString
      val pattern = new Regex("\\d+\\-\\d+")
      val dataSome = pattern findFirstIn str
      if (dataSome.isEmpty) {
        0.0
      } else {
        val ranges: List[(String, Double)] = List(("0-100", 0.8), ("100-500", 1.6), ("500-1000", 2.4), ("1000-10000", 3.2), ("10000-1000000", 4))
        val result = dataSome.getOrElse("0-0")
        val minMax = result.split("-")
        if (minMax.length == 2) {
          rangeJudge(minMax(0).toDouble + (minMax(1).toDouble - minMax(0).toDouble) / 2, ranges)
        } else {
          0.0
        }
      }
    }
  }

  // 逻辑判断评分
  def boolJudge(num: Any, boolMark: (Double, Double)): Double = {
    if (num == null || num.toString == "") {
      0.0
    } else {
      val str = num.toString
      val pattern = new Regex("\\d+")
      val dataSome = pattern findFirstIn str
      if (dataSome.isEmpty) {
        0.0
      } else {
        val result = dataSome.getOrElse("0.0").toDouble
        if (result > 0) boolMark._2 else boolMark._1
      }
    }
  }


}
