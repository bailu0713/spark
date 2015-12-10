package com.ctvit.streaming

import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._

/**
 * Created by BaiLu on 2015/7/9.
 */
object LogsWordCount {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("logwordcount")
    val stc = new StreamingContext(conf, Seconds(10))
    //    val lines=stc.socketTextStream("10.3.3.182",8080)

//    count lines of file through spark streaming
    val lines = stc.textFileStream(args(0))
    val line = lines.map(line => ("line", 1))
    val linecounts = line.reduceByKey(_ + _)


    //    val words = lines.flatMap(_.split("::"))
    //    val pairs = words.map(word => (word, 1))
    //    val wordCounts = pairs.reduceByKey(_ + _)

    // Print the first ten elements of each RDD generated in this DStream to the console
    //    wordCounts.saveAsTextFiles("/user/bl/streamingtest")
//    linecounts.saveAsTextFiles("/user/bl/streamingtest")
    println("###################################")
    println("###################################")
    println("###################################")
    linecounts.print()
    println(linecounts.toString.toInt)
    println("###################################")
    println("###################################")
    println("###################################")
    stc.start() // Start the computation
    stc.awaitTermination()

  }

}
