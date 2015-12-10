package com.ctvit.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.StreamingContext._
/**
 * Created by BaiLu on 2015/7/9.
 */
object TopN {

  def main(args:Array[String]): Unit ={

    val conf=new SparkConf().setMaster("local[*]").setAppName("topN")
    val stc=new StreamingContext(conf,Seconds(30))

    val lines=stc.textFileStream("hdfs://lab01:8020/user/bl/movielens/ml-1m/ratings.dat")
    val topN=lines.map{line=>val field=line.split("::");(field(1),1)}.reduceByKey(_+_).map(tup=>(tup._2,tup._1)).filter(tup=>tup._1<100).transform(tup=>tup)


  }

}
