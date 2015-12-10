package com.ctvit.sql

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.SparkContext._
/**
 * Created by BaiLu on 2015/7/17.
 */
object ContentHot {

  case class CidCount(contentId:String,hotCount:Int)

  def main(args:Array[String]): Unit ={

    val conf=new SparkConf().setMaster("local").setAppName("contentHot")
    val sc=new SparkContext(conf)
    val sqlContext=new SQLContext(sc)
    import sqlContext._

    val rawRdd=sc.textFile("hdfs://lab01:8020/user/zyl/vsp/vod/*.csv")


    val cidHot=rawRdd.map{line=>val field=line.split(",");(field(5),1)}.reduceByKey(_+_).map(tup=>CidCount(tup._1.toString,tup._2.toInt))
//    cidHot.registerTempTable("cidcount")/////////
    val sqlRes=sqlContext.sql("select contentId,hotCount from cidcount order by hotCount desc limit 100 ")
    sqlRes.collect().foreach(println)
//    cidHot.foreach(println)
//    println("#####",cidHot,"#####")
//    cidHot.foreach(println)

  }

}
