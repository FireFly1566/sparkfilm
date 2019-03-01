package datacleaner

import conf.AppConf
import org.apache.spark.sql.SaveMode

object PopulerMovies extends AppConf {

  def main(args: Array[String]): Unit = {
    val moviesRatingCount = hc.sql("select count(*) c,movieid from trainingdata group by movieid order by c desc")
    val top5 = moviesRatingCount.limit(5)
    top5.registerTempTable("top5")
    val top5DF = hc.sql("select a.title from movies a join top5 b on a.movieid=b.movieid")

    top5DF.write.mode(SaveMode.Overwrite).parquet("/tmp/top5_parquet")
    hc.sql("drop table if exists top5")
    hc.sql("create table if not exists top5(title string) stored as parquet")
    hc.sql("laod data inpath '/tmp/top5_parquet' overwrite into table top5")


  }

}
