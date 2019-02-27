package conf

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

}
