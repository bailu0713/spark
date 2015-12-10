package com.ctvit.recommendation


import java.text.SimpleDateFormat
import java.util.Calendar
import java.util.regex.Pattern

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.SparkContext._

/**
 * Created by BaiLu on 2015/7/2.
 */
object OneWeekVsOneMonth {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("OneWeekVsOneMonth")
    val sc = new SparkContext(conf)
    val date = weekSpan()
    val src = sc.textFile("hdfs://192.168.168.41:8020/user/zyl/vsp/vod/*.csv")
//    val src = sc.textFile("hdfs://192.168.168.41:8020/user/bl/movielens/ml-10m/ratings.dat")
//        val src = sc.textFile(s"hdfs://192.168.168.41:8020/user/zyl/vsp/vod/{$date}.csv")
//    val src = sc.textFile("e:\\log.csv")
    val srcRdd = src.map { line => val value = line.split(","); ((value(6), value(5)), 1)}.filter { value => Pattern.matches("\\d+", value._1._1)}.filter { value => Pattern.matches("\\d+", value._1._2)}.reduceByKey(_ + _).map(value => Rating(value._1._1.toInt, value._1._2.toInt, value._2.toDouble))
//val srcRdd = src.map { line => val value = line.split("::"); Rating(value(0).toInt, value(1).toInt, value(2).toDouble)}
//    val srcRdd = src.map { line => val value = line.split(","); ((value(10), value(11)), 1)}.filter { value => Pattern.matches("\\d+", value._1._1)}.filter { value => Pattern.matches("\\d+", value._1._2)}.reduceByKey(_ + _).count()
//    println(srcRdd)
    val predictRdd = srcRdd.map { case Rating(caid, movieid, score) => (caid, movieid)}
    val rank = 10
    val numIterations = 20
    //    ALS
    val model = ALS.train(srcRdd, rank, numIterations, 0.01)
    val predictions = model.predict(predictRdd).map { case Rating(user, product, rate) => ((user, product), rate)}


    val hadoopconf = new Configuration()
    val fs = FileSystem.get(hadoopconf)

    val dstPath = "hdfs://192.168.168.41:8020/user/bl/weekvsmonth"
    if (fs.exists(new Path(dstPath)))
      fs.delete(new Path(dstPath), true)
    predictions.saveAsTextFile(dstPath)

    //    predictions.foreach(println)


  }

  def tripleScore(count: Int): Int = {
    if (count > 0 && count <= 5)
      1
    if (count > 5 && count <= 10)
      2
    if (count > 10 && count <= 15)
      3
    else
      5
  }

  def weekSpan(): String = {
    val df = new SimpleDateFormat("yyyyMMdd")
    val buffer = new StringBuffer()

    for (i <- 1 to 7) {
      val calendar = Calendar.getInstance()
      calendar.add(Calendar.DAY_OF_WEEK, -i - 15)
      buffer.append(df.format(calendar.getTime) + ",")
    }
    buffer.toString
  }

}
