import java.text.SimpleDateFormat
import java.util.{Date, Random}

import com.alibaba.fastjson.JSON
import org.apache.spark.sql.functions.rand
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import sparkSql.CompanyJudgeScoring.rangeJudge

import scala.util.matching.Regex

object SparkTest {
  def main(args: Array[String]): Unit = {

//    val spark = SparkSession.builder()
//      .master("local")
//      .appName("test")
//      .getOrCreate()
//    import spark.implicits._
//
//    spark.read.textFile("C:\\Users\\ChuangLan\\Desktop\\remarks_category.txt").map(v => {
//      val remarks = v.split("\t")
//      (remarks(0),remarks)
//    }).show(50)
//    println(
//      """
//        |SELECT count(c.company_name)
//        |FROM
//        |  (SELECT a.company_name
//        |   FROM
//        |     (SELECT DISTINCT company_name
//        |      FROM ci_ods.ods_spider_qcc_company_product_remarks
//        |      WHERE ptt_day='20191210') a
//        |   JOIN
//        |     (SELECT DISTINCT company_name
//        |      FROM ci_ods.ods_spider_qcc_company_base_register
//        |      WHERE ptt_day='20191210'
//        |        AND business_status NOT LIKE '%销%') b ON a.company_name=b.company_name) c
//        |LEFT JOIN
//        |  (SELECT DISTINCT company_name
//        |   FROM ci_ods.ods_erp_tek_customer_f) d ON c.company_name=d.company_name
//        |WHERE d.company_name IS NULL;
//      """.stripMargin)

//    println(new Regex("\\d+").findFirstMatchIn("123").getOrElse(0).toString.toInt)
//
//    val tuples: List[(String, Int)] = List(("元人民币", 1), ("万加元", 2), ("万日元", 3))
//    println(tuples.find(_._1=="232s").getOrElse(("",1))._2)
//    print(reg_capital_judge("200万日元"))
//    var parse = new SimpleDateFormat("yyyy-MM-dd")
//    val param= "{}"
//    JSON.parseObject(param).getString("safds")

    println(new Random().nextDouble())

  }


  def reg_capital_judge(reg_capital : Any) : Double = {
    if (reg_capital == null || reg_capital.toString == ""){
      0.0
    }else{
      val str = reg_capital.toString
      val pattern = new Regex("\\d+")
      val dataSome = pattern findFirstIn str
      if (dataSome.isEmpty){
        0.0
      }else{
        val ranges: List[(String, Double)] = List(("0-100", 1.0), ("100-500", 2.0), ("500-1000", 3.0))
        val unit = new Regex("\\D+").findFirstMatchIn(str).getOrElse("")
        val tuples: List[(String, Double)] = List(("元人民币", 1.0), ("万加元", 2.0), ("万日元", 3.0))
        val result = dataSome.getOrElse(0).toString.toInt * tuples.find(_._1.equals(unit.toString)).getOrElse(("",1.0))._2.toDouble
        rangeJudge(result,ranges)
      }
    }
  }

  def rangeJudge(num : Double , rangeMark : List[(String, Double)]): Double = {
    if (num == 0){
      0.0
    }else{
      rangeMark.find(v =>{
        val minMax = v._1.split("-")
        if (minMax.length == 2){
          num > minMax(0).toDouble && num <= minMax(1).toDouble
        }else{
          false
        }
      }).getOrElse(("",0.0))._2.toDouble
    }
  }


  def yearJudge(establish_date : Any) : Double = {
    if (establish_date == null || establish_date.toString == ""){
      0.0
    }else{
      val str = establish_date.toString
      val pattern = new Regex("\\d{4}-\\d{2}-\\d{2}")
      val dataSome = pattern findFirstIn str
      if (dataSome.isEmpty){
        0.0
      }else{
        val parse = new SimpleDateFormat("yyyy-MM-dd")
        val year = (parse.parse(parse.format(new Date)).getTime - parse.parse(dataSome.getOrElse("9999-99-99").toString).getTime).toDouble/ 1000 / 3600 / 24 / 365
        val ranges: List[(String, Double)] = List(("0-3", 0.8), ("3-5", 1.6), ("5-10", 2.4),("10-20",3.2),("20-1000000",4))
        rangeJudge(if (year > 0 ) year else  0.0,ranges)
      }
    }
  }


  def numJudge(employeeNum : Any) : Double = {
    if (employeeNum == null || employeeNum.toString == ""){
      0.0
    }else{
      val str = employeeNum.toString
      val pattern = new Regex("\\d+\\-\\d+")
      val dataSome = pattern findFirstIn str
      if (dataSome.isEmpty){
        0.0
      }else{
        val ranges: List[(String, Double)] = List(("0-100", 0.8), ("100-500", 1.6), ("500-1000", 2.4),("1000-10000",3.2),("10000-1000000",4))
        val result = dataSome.getOrElse("0-0")
        val minMax = result.split("-")
        if (minMax.length == 2){
          rangeJudge((minMax(1).toDouble - minMax(0).toDouble)/2,ranges)
        }else{
          0.0
        }
      }
    }
  }

}
