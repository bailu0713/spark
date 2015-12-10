package com.ctvit.recommendation

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.mllib.recommendation.{Rating, ALS}
import org.apache.spark.SparkContext._

/**
 * Created by BaiLu on 2015/3/12.
 */
object TrainTest {
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("TRAIN-TEST")
    val sc = new SparkContext(conf)
    val trainrdd = sc.textFile("hdfs://lab01:8020/user/bl/movielens/ml-1m/ratings.dat")
    val train = trainrdd.map(_.split("::")).map(field => Rating(field(0).toInt, field(1).toInt, field(2).toDouble))
    val model = ALS.train(train, 10, 20, 0.01)

    val testrdd = sc.textFile("hdfs://lab01:8020/user/bl/movielens/ml-10m/rating.dat")
    val test = testrdd.map { line => val field = line.split("::"); (field(0).toInt, field(1).toInt, field(2).toDouble)}
    val prediction = test.map { case (user, item, rating) => (user, item)}
    val res = model.predict(prediction).map { case Rating(user, item, rate) => ((user, item), rate)}
    val MSE = test.map { case (user, item, rate) => ((user, item), rate)}.join(res).map { case ((user, item), (r1, r2)) => Math.pow(r1 - r2, 2)}


    val outputpath=s"hdfs://192.168.168.41:8020/user/bl/movielens/cluster"
    val configuration=new Configuration()
    val fs=FileSystem.get(configuration)
    if(fs.exists(new Path(outputpath)))
      fs.delete(new Path(outputpath))
    MSE.saveAsTextFile(outputpath)


    println("MSE IS: " + MSE)
  }

}
