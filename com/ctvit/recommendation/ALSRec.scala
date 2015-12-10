package com.ctvit.recommendation

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._

/**
 * Created by BaiLu on 2015/3/3.
 */
object ALSRec {
  def main(args: Array[String]): Unit = {

    //    if (args.length != 1) {
    //      println("you need to specify the hdfs path of ratings.dat")
    //      println("something like: !!! hdfs://lab01:8020/user/bl/data/movielens/ml-10m/ratings.dat !!!")
    //      System.exit(-1)
    //    }

    //    val conf = new SparkConf().setAppName("MovieLensALS").setMaster("local").set("spark.driver.memory","8g").set("spark.executor.memory","24g")
    val conf = new SparkConf().setAppName("ALSRec")
    val sc = new SparkContext(conf)

    //    val rdd = sc.textFile("hdfs://192.168.168.41:8020/user/bl/movielens/ml-10m/ratings.dat")
    //      val ratings = rdd.map { line => val fields = line.split("::"); Rating(fields(0).toInt, fields(1).toInt, fields(2).toDouble)}
    val rdd = sc.textFile("hdfs://192.168.168.41:8020/user/zyl/vsp/vod/*.csv")
    val ratings = rdd.filter(line => line.split(",").length > 7)
      .map { line => val fields = line.split(","); ((fields(7).toInt, fields(6).toInt), 1.0)}.reduceByKey(_ + _)
      .map(tup => Rating(tup._1._1, tup._1._2, tup._2))
    val rank = 10
    val numIterations = 20
    //    ALS
    val model = ALS.train(ratings, rank, numIterations, 0.01)
    val usersProducts = ratings.map { case Rating(user, product, rate) => (user, product)}
    val predictions = model.predict(usersProducts).map { case Rating(user, product, rate) => ((user, product), rate)}

    //    save prediction
    val date = today()
    val outputpath = s"hdfs://192.168.168.41:8020/user/bl/als/$date"
    val configuration = new Configuration()
    val fs = FileSystem.get(configuration)
    if (fs.exists(new Path(outputpath)))
      fs.delete(new Path(outputpath), true)
    predictions.saveAsTextFile(outputpath)
    val ratesAndPreds = ratings.map { case Rating(user, product, rate) => ((user, product), rate)}.join(predictions)
    val MSE = ratesAndPreds.map { case ((user, product), (r1, r2)) => val err = r1 - r2; err * err}.mean()
    //    val MSE = ratesAndPreds.map { case ((user, product), (r1, r2)) => Math.pow(r1 - r2, 2)}.mean()
    println("Mean Squared Error = " + MSE)

  }

  def today(): String = {
    val df = new SimpleDateFormat("yyyyMMdd")
    val calendar = Calendar.getInstance()
    df.format(calendar.getTime)
  }

}
