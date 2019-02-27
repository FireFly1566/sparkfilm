package datacleaner

import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object RatingData {
  def main(args: Array[String]): Unit = {
    val localURl = "local[2]"
    val clusterMasterURL = "spark://node1:7077"
    val conf = new SparkConf().setAppName("RatingData").setMaster(clusterMasterURL)
    val sc = new SparkContext(conf)
    val hc = new HiveContext(sc)

    //RDD[Rating] 从原始数据表中提取 userid,movieid,rating数据
    // 并把这些数据切分成训练集和测试数据集
    val ratings = hc.sql("cache table ratings")
    val count = hc.sql("select count(*) from ratings").first().getLong(0).toInt
    val percent = 0.6
    val trainingdatacount = (count * percent).toInt
    val testdatacount = (count * (1 - percent)).toInt

    // oreder by limit 的时候，需要注意 OOM 问题
    val traingDataAsc = hc.sql(s"select userId,movieId,rating from ratings order by timestamp asc")
    traingDataAsc.write.mode(SaveMode.Overwrite).parquet("/tmp/trainingDataAsc")
    hc.sql("drop table if exists trainingDataAsc")
    hc.sql("create table if not exists trainingDataAsc(userId int,movieId int,rating double) stored as parquet")
    hc.sql("load data inpath '/tmp/trainingDataAsc' overwrite into table trainingDataAsc")

    val traingDataDesc = hc.sql(s"select userId,movieId,rating from ratings order by timestamp desc")
    traingDataDesc.write.mode(SaveMode.Overwrite).parquet("/tmp/trainingDataDesc")
    hc.sql("drop table if exists trainingDataDesc")
    hc.sql("create table if not exists trainingDataDesc(userId int,movieId int,rating double) stored as parquet")
    hc.sql("load data inpath '/tmp/trainingDataDesc' overwrite into table trainingDataDesc")

    val trainingData = hc.sql(s"select * from trainingDataAsc limit $trainingdatacount")
    trainingData.write.mode(SaveMode.Overwrite).parquet("/tmp/trainingData")
    hc.sql("drop table if exists trainingData")
    hc.sql("create table if not exists trainingData(userId int,movieId int,rating double) stored as parquet")
    hc.sql("load data inpath '/tmp/trainingData' overwrite into table trainingData")

    val testData = hc.sql(s"select * from trainingDataDesc limit $testdatacount")
    testData.write.mode(SaveMode.Overwrite).parquet("/tmp/testData")
    hc.sql("drop table if exists testData")
    hc.sql("create table if not exists testData(userId int, movieId int,rating double) stored as parquet")
    hc.sql("load data inpath '/tmp/testData' overwrite into table testData")


    val ratingRDD = hc.sql("select * from trainingData").rdd.map(x => Rating(x.getInt(0),x.getInt(1),x.getDouble(2)))


    // 第二个参数 rank : 特征向量的个数（此处就是电影的评分）
    val model = ALS.train(ratingRDD,1,10)

  }
}
