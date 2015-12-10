package com.ctvit.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}
import org.apache.spark.streaming.StreamingContext._

/**
 * Created by BaiLu on 2015/7/9.
 */
object UpdateTopN {
  def main(args:Array[String]): Unit ={

    if(args.length<1){
      println("Usage: NetworkWordCount <hostname> <port>")
      System.exit(1)
    }

//    val rdd2 = rdd1.reduceByKey(_ + _, numPartitions = X)

    val conf=new SparkConf().setAppName("topN").setMaster("local[*]")
    val stc=new StreamingContext(conf,Minutes(160))
    stc.checkpoint("hdfs://192.168.168.41:8020/user/bl/")

    val updateFun=(currentValues:Seq[Int],preValueState:Option[Int])=>{
      val currentSum=currentValues.sum
      val previousSum=preValueState.getOrElse(0)
      Some(currentSum+previousSum)
    }

//    val lines=stc.socketTextStream(args(0),args(1).toInt)
    val lines = stc.textFileStream(args(0))
    val pairs=lines.flatMap{line=>line.split("::")}.map(word=>(word,1))

    val totalWordCount=pairs.updateStateByKey[Int](updateFun)
    totalWordCount.print()
    stc.start()
    stc.awaitTermination()


  }

}
