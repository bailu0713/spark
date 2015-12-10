package com.ctvit.recommendation

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by BaiLu on 2015/3/12.
 */
object Cartesian {
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local").setAppName("Cartesian")
    val sc = new SparkContext(conf)

    val rdd = sc.textFile("hdfs://lab01:8020/user/bl/data/movielens/ml-1m/ratings.dat").map { line => val field = line.split("::"); (field(0), field(1), field(2))}
    val left = rdd.map { case (user, item, rating) => user}.distinct()
    val right = rdd.map { case (user, item, rating) => item}.distinct()
    val res = left.cartesian(right)
    res.saveAsTextFile("/user/bl/data/movielens/cartesian")

  }

}
