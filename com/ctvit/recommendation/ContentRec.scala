package com.ctvit.recommendation

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by BaiLu on 2015/4/27.
 */
object ContentRec {
  def main(args: Array[String]) = {
    val conf = new SparkConf().setAppName("ContentRec").setMaster("local")
    val sc = new SparkContext(conf)
    val rdd=sc.textFile("hdfs://192.168.168.162:/user/bl/data/")

  }


}
