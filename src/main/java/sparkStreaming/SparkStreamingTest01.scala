package sparkStreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreamingTest01 {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Test").setMaster("local[*]")
    val ssc = new StreamingContext(conf,Seconds(1))

    val line = ssc.socketTextStream("172.16.20.130",6666)
    val words = line.flatMap(_.split(" "))
    val pairs = words.map((_,1))
    val wordCount = pairs.reduceByKey(_+_)

    wordCount.print()
    ssc.start()
    ssc.awaitTermination()

  }
}
