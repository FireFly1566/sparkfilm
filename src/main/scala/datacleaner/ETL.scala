package datacleaner

import caseclass.{Links, Movies, Ratings, Tags}
import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object ETL {

  def main(args: Array[String]): Unit = {

    val localURL = "local[2]"
    val clusterMasterURL = "spark://node1:7077"

    val conf = new SparkConf().setAppName("ETL").setMaster(clusterMasterURL)
    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)

    val hc = new HiveContext(sc)

    import sqlContext.implicits._


    // 设置 RDD 的partition 的数量一般以集群分配给应用CPU核数的整数倍
    val minPartitions = 8

    // 通过 case class 定义 Links数据结构，数据 schema，适用 schema 已知
    // 也可以通过 StructType 方式，适用 schema 未知
    val links = sc.textFile("hdfs://node1:8020/moviedata/links.txt", minPartitions) // Driver端
      .filter(!_.endsWith(",")) // Executor
      .map(_.split(",")) // Executor
      .map(x => Links(x(0).trim.toInt, x(1).trim.toInt, x(2).trim.toInt)) // Executor
      .toDF()



    val movies = sc.textFile("hdfs://node1:8020/moviedata/movies.txt", minPartitions)
      .filter{!_.endsWith(",")}
      .map(_.split(","))
      .map(x => Movies(x(0).trim.toInt, x(1).trim, x(2).trim))
      .toDF()

    val ratings = sc.textFile("hdfs://node1:8020/moviedata/ratings.txt", minPartitions)
      .filter(!_.endsWith(","))
      .map(_.split(","))
      .map(x => Ratings(x(0).trim.toInt, x(1).trim.toInt, x(2).trim.toDouble, x(3).trim.toInt))
      .toDF()

    val tags = sc.textFile("hdfs://node1:8020/moviedata/tags.txt", minPartitions)
      .filter(!_.endsWith(","))
      .map(x => rebuild(x))
      .map(_.split(","))
      .map(x => Tags(x(0).trim.toInt, x(1).trim.toInt, x(2).trim, x(3).trim.toInt))
      .toDF()




    // 在 SQLContext 下只能构建临时表
    // links.registerTempTable("links0629")

    // 通过数据写入到HDFS,将表存到 hive 中
    // 数据写到 HDFS
    links.write.mode(SaveMode.Overwrite).parquet("hdfs://node1:8020/tmp/links")
    hc.sql("drop table if exists links")
    hc.sql("create table if not exists links(movieId int,imdbId int,tmdbId int) stored as parquet")
    hc.sql("load data inpath 'hdfs://node1:8020/tmp/links' overwrite into table links")


    // Movies(movieId:Int,title:String,genres:String)
    movies.write.mode(SaveMode.Overwrite).parquet("hdfs://node1:8020/tmp/movies")
    hc.sql("drop table if exists movies")
    hc.sql("create table if not exists movies(movieId int,title string,genres string) stored as parquet")
    hc.sql("load data inpath 'hdfs://node1:8020/tmp/movies' overwrite into table movies")

    // Ratings(userId:Int,movieId:Int,rating:Double,timestamp:Int)
    ratings.write.mode(SaveMode.Overwrite).parquet("/tmp/ratings")
    hc.sql("drop table if exists ratings")
    hc.sql("create table if not exists ratings(userId int,movieId int,rating double,timestamp int) stored as parquet")
    hc.sql("load data inpath '/tmp/ratings' overwrite into table ratings")

    // Tags(userId:Int,movieId:Int,tag:String,timestamp:Int)
    tags.write.format("parquet").mode(SaveMode.Overwrite).save("hdfs://node1:8020/tmp/tags")
    hc.sql("drop table if exists tags")
    hc.sql("create table if not exists tags(userId int,movieId int,tag string,timestamp int) stored as parquet")
    hc.sql("load data inpath '/tmp/tags' overwrite into table tags")

  }


  /*
  * 处理错误格式的数据
  * 4208,260,“Family,Action-packed”,1438012562
  *
  * */
  private def rebuild(str: String): String = {
    // 得到一个字符串数组
    val a = str.split(",")

    // 取前2个元素
    val head = a.take(2).mkString(",")

    val tail = a.takeRight(1).mkString

    val b = a.drop(2).dropRight(1).mkString.replace("\"", "")

    val output = a + "," + b + "," + tail

    output
  }


}
