package com.ctvit.spark.MLlib

import org.apache.spark.mllib.feature.{IDF, HashingTF}
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by BaiLu on 2015/7/29.
 */
object HashTF {

  def main(args:Array[String]): Unit ={

    val conf=new SparkConf().setAppName("TFIDF").setMaster("local[*]")
    val sc=new SparkContext(conf)
//    val documents = sc.textFile("hdfs://192.168.168.41:8020/user/bl/mllib/sample_svm_data.txt").map(_.split(" ").toIterable)
    val documents = sc.textFile("hdfs://192.168.168.41:8020/user/bl/mllib/sample_svm_data.txt").map(_.split(" ").toSeq)
    val hashingTF = new HashingTF()
    val tf = hashingTF.transform(documents)
    tf.cache()
    val idf = new IDF(minDocFreq = 2).fit(tf)
    val tfidf = idf.transform(tf)
    tfidf.collect().foreach(println)

  }

}
