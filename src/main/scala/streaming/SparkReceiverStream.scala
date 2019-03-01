package streaming

import conf.AppConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkReceiverStream extends AppConf {

  def main(args: Array[String]): Unit = {

    val ssc = new StreamingContext(sc, Seconds(5))

    val validusers = hc.sql("select * from trainingData")

    val modelpath = "/tmp/BestModel/0.8578486285109932"

    val broker = "node1:9092,node2:9092,node3:9092"

    val topics = Map("test" -> 1)

    val kafkaParams = Map("broker" -> "node1:9092,node2:9092,node3:9092")

    val zkQuorum = "node1:2181,node2:2181,node3:2181"
    val groupId = 1
    val storageLevel = StorageLevel.MEMORY_ONLY

    // 创建 SparkStreaming 接收 kafka 消息队列的第2种方式
    //  Receiver base approach,把 sparkstreaming当做一个 consumer 来消费 kafka中的消息
    // 通过启用 WAL 的方式把这个 stream 做成强一致性
    val kafkaStream = KafkaUtils.createStream(ssc, zkQuorum, "1", topics, storageLevel)

  }

}
