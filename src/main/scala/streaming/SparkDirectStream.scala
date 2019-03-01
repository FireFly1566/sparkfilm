package streaming

import kafka.serializer.StringDecoder
import org.apache.spark.{SparkConf}
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkDirectStream {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("SparkDirectStream").setMaster("spark://node1:7077")

    val ssc = new StreamingContext(conf, Seconds(5))

    val hc = new HiveContext(ssc.sparkContext)

    val validusers = hc.sql("select * from trainingData")
    val userlist = validusers.select("userId")

    val modelpath = "/tmp/BestModel/0.8578486285109932"
    val broker = "node1:9092,node2:9092,node3:9092"
    val topics = "test".split(",").toSet
    val kafkaParams = Map("bootstrap.servers" -> "node1:9092,node2:9092,node3:9092")

    def exists(u: Int): Boolean = {
      val userlist = hc.sql("select distinct(userid) from trainingdata").rdd.map(_.getInt(0)).toArray()
      userlist.contains(u)
    }

    val defaultrecsult = hc.sql("select * from top5").rdd.toLocalIterator

    // 为没有登录的用户推荐电影的策略
    // 1. 推荐观看人数较多的电影，此处用这种
    // 2. 推荐最新的电影
    //    def recommendPopularMovies(): Unit = {
    //      hc.sql("select * from top5").show()
    //    }

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

      val model = MatrixFactorizationModel.load(ssc.sparkContext, modelpath)

      val userrdd = rdd.map(x => x._2.split("|")).map(x => x(1)).map(_.toInt)
      val validusers = userrdd.filter(user => exists(user))
      val newusers = userrdd.filter(user => !exists(user))

      // 可以采用迭代器的方式避开对象不能序列化的问题，通过对RDD中每个元素实时产生推荐结果，
      // 将结果写入到redis，或者其他高速缓存中，来达到一定的实时性。

      val validuserIter = validusers.toLocalIterator
      val newusersIter = newusers.toLocalIterator

      while (validuserIter.hasNext) {
        val recresult = model.recommendProducts(validuserIter.next(), 5)
        println("以下是推荐的结果：")
        println(recresult)
      }

      while (newusersIter.hasNext) {
        println("以下是为 newers 推荐的：")
        for (i <- defaultrecsult) {
          println(i.getString(0))
        }
      }

    }

    ssc.start()
    ssc.awaitTermination()

  }

}
