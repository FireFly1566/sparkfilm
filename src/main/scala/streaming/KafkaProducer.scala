package streaming

import java.util.Properties

import conf.AppConf
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}


// spark-submit --class streaming.KafkaProducer /home/hadoop/spark-film-1.0.jar --jars /home/hadoop/lib/kafka-clients-0.9.0.0.jar

object KafkaProducer extends AppConf {
  def main(args: Array[String]): Unit = {

    val testDF = hc.sql("select * from testData")

    val prop = new Properties()
    prop.put("bootstrap.servers", "node1:9092,node2:9092,node3:9092")
    prop.put("acks", "all")
    prop.put("retries", 0)
    prop.put("batch.size", 16384)
    prop.put("linger.ms", 1)
    prop.put("buffer.memory", 33554432)
    prop.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    prop.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val topic = "test"

    val testData = testDF.map { x =>
      (topic, x.getInt(0).toString + "|" + x.getInt(1).toString + "|" + x.getDouble(2).toString)
    }
    val producer = new KafkaProducer[String, String](prop)
    val messages = testData.toLocalIterator

    while (messages.hasNext) {
      val message = messages.next()
      val record = new ProducerRecord[String, String](topic, message._1, message._2)
      println(record)
      producer.send(record)
      Thread.sleep(1000)

    }


    // 为什么不使用 testData.map 或者 foreach?
    // 因为这 2 种方法会让数据做分布式运算，在计算时，处理数据是无序的

    // 使用 for 的方式，会出现序列化的问题
    //    for (x <- testData) {
    //      val message = x
    //      val record = new ProducerRecord[String, String]("test", message._1, message._2)
    //      println(record)
    //      producer.send(record)
    //    }

    producer.close

  }
}
