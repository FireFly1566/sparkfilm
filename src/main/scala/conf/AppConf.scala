package conf

import java.util.Properties

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

trait AppConf {
  val localURl = "local[2]"
  val clusterMasterURL = "spark://node1:7077"
  val conf = new SparkConf().setMaster(clusterMasterURL)

  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)

  val hc = new HiveContext(sc)

  val jdbcURL = "jdbc:mysql://node1:3306/hive_db?useSSL=false"
  val recResultTable = "hive_db.user_movie_recommandation"
  val mysqlusername = "root"
  val mysqlpassword = "root"

  val prop = new Properties()
  prop.put("drver","com.mysql.jdbc.Driver")
  prop.put("user",mysqlusername)
  prop.put("password",mysqlpassword)



}
