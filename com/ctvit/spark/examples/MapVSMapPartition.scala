package com.ctvit.spark.examples

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by BaiLu on 2015/12/3.
 */
object MapVSMapPartition {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("map").setMaster("local[*]")
    val sc = new SparkContext(conf)


    sc.parallelize(1 to 10, 3)
      //mappartition是对每一个分区进行一次的调用
      .mapPartitions(mapPartitions)
      .foreach(println)
  }

  def mapPartitions(iter: Iterator[Int]): Iterator[Int] = {
    println("run in partition")
    var res = for (i <- iter) yield 2 * i
    res
  }

}
