package com.ctvit.redis

import org.apache.spark.{SparkConf, SparkContext}
import redis.clients.jedis.Jedis

/**
 * Created by BaiLu on 2015/7/29.
 */
object WriteToRedis {

  val HDFS_DIR="hdfs://192.168.168.41/user/zyl/xorlog/vod/20150701.csv"
  val REDIS_IP="192.168.168.42"
  val REDIS_PORT=6379

  def main(args:Array[String]): Unit ={

    val startTime=System.nanoTime()
    val conf=new SparkConf().setAppName("ReadFromHDFS WriteToRedis").setMaster("local[*]")
    val sc=new SparkContext(conf)
//    val jedis=init()
    readFromHdfs(sc,conf)
//    jedis.set("bailu","bailu3")
    println("总的运行时间是："+(System.nanoTime()-startTime)/1e9+" seconds")
  }

  def init(redisip:String,redisport:Int): Jedis ={
    val jedis=new Jedis(redisip,redisport)
    jedis
  }
  def readFromHdfs(sc:SparkContext,conf:SparkConf): Unit ={
    val jedis=init(REDIS_IP,REDIS_PORT)
/**
 * list insert into redis
 * */
    sc.textFile(HDFS_DIR).map{line=>val values=line.split(",");(values(0),values(2))}
      .collect()
      .foreach{case(key,value)=>jedis.mset(key,value)}
  }

}
