package ml

import conf.AppConf
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel

object Recommender extends AppConf {

  def main(args: Array[String]): Unit = {

    // 从 trainingData 中取得 userId 一定存在于模型中
    val users = hc.sql("select distinct(userId) from trainingData order by userId asc")
    val index = 139
    val uid = users.take(index).last.getInt(0)

    val modelpath = "/tmp/BestModel/0.8616887644293227"
    val model = MatrixFactorizationModel.load(sc, modelpath)

    //推荐 5 个产品
    val rec = model.recommendProducts(uid, 5);

    // Rating(user: Int, product: Int, rating: Double)
    val recmovieid = rec.map(_.product)


    println("我为用户 " + uid + " 推荐的5部电影是：")

    for (i <- recmovieid) {
      // 取 DataFrame 的第一行
      val moviename = hc.sql(s"select title from movies where movieId=$i").first().getString(0)
      println(moviename)
    }

//    recmovieid.map { x =>
//      val moviename = hc.sql(s"select title from movies where movieId=$x").first().getString(0)
//      println(moviename)
//    }


  }
}
