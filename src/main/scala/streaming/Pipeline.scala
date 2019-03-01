package streaming

import conf.AppConf
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.recommendation.ALS

/**
  *
  * 基于 DataFrame 构建 Spark ML应用
  *
  *  spark.ml 是基于 DataFrame 来构建 pipeline,
  * 通过 pipeline 来完成机器学习的流水线
  *
  **/

object Pipeline extends AppConf {
  def main(args: Array[String]): Unit = {

    val trainingData = hc.sql("select * from trainingdata")
      .withColumnRenamed("userid", "user")
      .withColumnRenamed("movieid", "item")

    val testData = hc.sql("select * from testdata")
      .withColumnRenamed("userid", "user")
      .withColumnRenamed("movieid", "item")

    trainingData.cache()
    testData.cache()

    // 构建一个 estimator
    val als = new ALS().setMaxIter(20).setRegParam(1.0).setRank(1)

    val p = new Pipeline().setStages(Array(als))

    val model = p.fit(trainingData)


    val test = model.transform(testData).select("rating", "prediction")

    val MSE = test.map(x => math.pow(x.getDouble(0) - x.getFloat(1), 2)).mean()
    val RMSE = math.sqrt(MSE)

    model.save("/tmp/ml/ALSmodel")

  }
}
