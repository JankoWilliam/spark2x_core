package sparkStreaming

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.dstream.{DStream, InputDStream, MapWithStateDStream}
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.StateSpec

import scala.util.matching.Regex


object SparkStreamingKafkaTest01 {
  def main(args: Array[String]): Unit = {
    // 1.创建SparkConf对象
    val conf: SparkConf = new SparkConf()
      .setAppName("SparkStreamingKafka_Direct")
      .setMaster("local[*]")

    // 2.创建SparkContext对象
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    // 3.创建StreamingContext对象
    /**
      * 参数说明：
      * 参数一：SparkContext对象
      * 参数二：每个批次的间隔时间
      */
    val ssc: StreamingContext = new StreamingContext(sc, Seconds(5))
    //设置checkpoint目录

    ssc.checkpoint("./Kafka_Direct")

    // 4.通过KafkaUtils.createDirectStream对接kafka(采用是kafka低级api偏移量不受zk管理)
    // 4.1.配置kafka相关参数
    val kafkaParams = Map(
      "bootstrap.servers" -> "172.16.20.130:9092",
      "metadata.broker.list" -> "172.16.20.130:9092,172.16.20.131:9092,172.16.20.132:9092",
      "group.id" -> "kafka_Direct",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    // 4.2.定义topic
    val topics = Set("kafka_spark")

    val dstream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams))

    // 5.获取topic中的数据
    val topicData  = dstream.flatMap(record => record.value.split("\\n"))

    val logData = topicData.map(v =>{
      if (v.contains("ERROR")) ("ERROR",(new Regex("\\d+") findFirstIn v).getOrElse(0).toString.toInt )
      else if (v.contains("INFO")) ("INFO",(new Regex("\\d+") findFirstIn v).getOrElse(0).toString.toInt )
      else if (v.contains("WARNING")) ("WARNING",(new Regex("\\d+") findFirstIn v).getOrElse(0).toString.toInt )
      else ("any",0)
    })


    val result :MapWithStateDStream[String, Int, Int, Any] = logData.mapWithState(
    StateSpec.function((word, one, state) => {
      val sum = one.getOrElse(0) + state.getOption.getOrElse(0)
      val output = (word, sum)
      state.update(sum)
      output
    }))

    // 8.通过Output Operations操作打印数据
    result.print()

    // 9.开启流式计算
    ssc.start()

    // 阻塞一直运行
    ssc.awaitTermination()

  }
}
