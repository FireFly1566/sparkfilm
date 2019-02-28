package ml

import caseclass.Result
import conf.AppConf
import org.apache.spark.mllib.recommendation.{MatrixFactorizationModel, Rating}
import org.apache.spark.sql.SaveMode


/**
*  在集群中提交运行的时候，需要通过 --jars 把 mysql 驱动添加到 classpath
*
  * spark-submit --jars /home/hadoop/lib/mysql-connector-java-5.1.44-bin.jar --class ml.RecommendForAllUsers /home/hadoop/spark-film-1.0.jar
* */

object RecommendForAllUsers extends AppConf {

  def main(args: Array[String]): Unit = {

    // 从 trainingData 中取得 userId 一定存在于模型中
    val users = hc.sql("select distinct(userId) from trainingData order by userId asc")

    val allusers = users.rdd.map(_.getInt(0)).toLocalIterator

    // 方法 1， 可行，但是效率不高
    val modelpath = "/tmp/BestModel/0.8578486285109932"
    val model = MatrixFactorizationModel.load(sc, modelpath)
    while (allusers.hasNext) {

      // 推荐 5 个产品
      val rec = model.recommendProducts(allusers.next(), 5)

      writeRecResultToMysql(rec)

    }


    // 方法2，不可行,需要大量内存
    // val recResult = model.recommendProductsForUsers(5)

  }

  // https://spark.apache.org/docs/1.6.3/api/scala/index.html#org.apache.spark.sql.DataFrame
  // https://spark.apache.org/docs/1.6.3/api/scala/index.html#org.apache.spark.sql.DataFrameWriter
  def writeRecResultToMysql(rec: Array[Rating]): Unit = {

    val recString = rec.map(x =>
      x.user.toString + "|" + x.product.toString + "|" + x.rating.toString)


    import sqlContext.implicits._
    val recDF = sc.parallelize(recString)
      .map(_.split("|"))
      .map(x => Result(x(0).trim.toInt, x(1).trim.toInt, x(2).trim.toDouble))
      .toDF

    recDF.write.mode(SaveMode.Overwrite).jdbc(jdbcURL, recResultTable, prop)

  }

  def writeRecResultToHBase(rec: Array[Rating]): Unit ={

    val recString = rec.map(x =>
      x.user.toString + "|" + x.product.toString + "|" + x.rating.toString)

    import sqlContext.implicits._
    val recDF = sc.parallelize(recString)
      .map(_.split("|"))
      .map(x => Result(x(0).trim.toInt, x(1).trim.toInt, x(2).trim.toDouble))
      .toDF

    // 通过 phoenix 将 DataFrame 导入 HBase
    // https://phoenix.apache.org/phoenix_spark.html

    recDF.save("org.apache.phoenix.spark", SaveMode.Overwrite, Map("table" -> "phoenix_rec", "zkUrl" -> "localhost:2181"))

  }


}
