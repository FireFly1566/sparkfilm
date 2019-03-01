package streaming

import conf.AppConf
import kafka.serializer.StringDecoder
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkDirectStream extends AppConf {

  def main(args: Array[String]): Unit = {

    val ssc = new StreamingContext(sc, Seconds(5))

    val validusers = hc.sql("select * from trainingData")

    val modelpath = "/tmp/BestModel/0.8578486285109932"

    val broker = "node1:9092,node2:9092,node3:9092"

    val topics = "test".split(",").toSet

    val kafkaParams = Map("bootstrap.servers" -> "node1:9092,node2:9092,node3:9092")

    def exists(u: Int): Boolean = {
      val userlist = hc.sql("select distinct(userid) from trainingdata").rdd.map(_.getInt(0)).toArray()
      userlist.contains(u)
    }

    // 为没有登录的用户推荐电影的策略
    // 1. 推荐观看人数较多的电影，此处用这种
    // 2. 推荐最新的电影
    def recommendPopularMovies(): Unit = {
      hc.sql("select * from top5").show()
    }

    /*
    *  创建 SparkStreaming接收 kafka 消息队列数据的2种方式
    *
    * 1. Direct approache,通过 SparkStreaming 自己主动去 kafka消息队列
    *    中查询没有接收的数据，并把他们拿到 sparkstreaming,pull
    *
    * 2. Receivers 方式，
    * */
    val kafkaDirectStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)

    val messages = kafkaDirectStream.foreachRDD { rdd =>
      // val userrdd = rdd.map(x => x._2.split("|")).map(x => x(1)).map(_.toInt)
      //      rdd.foreach { record =>
      //        val user = record._2.split("|").apply(1).toInt
      //        val model = MatrixFactorizationModel.load(sc, modelpath)
      //        val recresult = model.recommendProducts(user, 5)
      //        println(recresult)
      //      }

      // 每一个 partition 对应一个 partition,在一个 executor执行
      // 在一个 executor 加载一次model
      rdd.foreachPartition { partition =>
        val userlist = partition.map(x => x._2.split("|")).map(x => x(1)).map(_.toInt)
        val model = MatrixFactorizationModel.load(sc, modelpath)

        while (userlist.hasNext) {
          val user = userlist.next()
          if (exists(user)) {
            val recresult = model.recommendProducts(user, 5)
            println(recresult)
          } else {
            println("为你推荐：")
            recommendPopularMovies()
          }

        }

      }


    }


  }

}
