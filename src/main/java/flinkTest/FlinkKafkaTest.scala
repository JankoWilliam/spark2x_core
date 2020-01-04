package flinkTest

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer

object FlinkKafkaTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "172.16.20.130:9092")
    // only required for Kafka 0.8
    properties.setProperty("zookeeper.connect", "172.16.20.130:9092,172.16.20.131:9092,172.16.20.132:9092")
    properties.setProperty("group.id", "kafka_Direct")

//    val kafkaParams = Map(
//      "bootstrap.servers" -> "172.16.20.130:9092",
//      "metadata.broker.list" -> "172.16.20.130:9092,172.16.20.131:9092,172.16.20.132:9092",
//      "group.id" -> "kafka_Direct",
//      "key.deserializer" -> classOf[StringDeserializer],
//      "value.deserializer" -> classOf[StringDeserializer],
//      "auto.offset.reset" -> "latest",
//      "enable.auto.commit" -> (false: java.lang.Boolean)
//    )

    val myConsumer = new FlinkKafkaConsumer[String]("kafka_spark",new SimpleStringSchema(),properties)
    //指定偏移量
    myConsumer.setStartFromEarliest()
    val stream = env.addSource(myConsumer)

    env.enableCheckpointing(5000)
    stream.print

    env.execute("Flink Streaming Java API Skeleton")

  }
}
