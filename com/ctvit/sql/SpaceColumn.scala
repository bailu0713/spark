package com.ctvit.sql

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.SparkContext._
/**
 * Created by BaiLu on 2015/7/23.
 */

/**
 * 空字符串在里面
 * */
object SpaceColumn {
  case class TempTable(contentId:String,genre:String,contentName:String,year:String,country:String,
                       directors:String,actors:String,seriesType:String)

  def main(args:Array[String]): Unit ={



    val conf=new SparkConf().setAppName("SpaceColumns").setMaster("local[*]")
    val sc=new SparkContext(conf)
    val sqlContext=new SQLContext(sc)
    import sqlContext._

    val rawRdd=sc.textFile("hdfs://lab01:8020/user/bl/data.txt")
    val content=rawRdd.map{line=>val field=line.split("\\|");TempTable(field(0),field(1),field(2),field(3),field(4),field(5),field(6),field(7))}
//    val content=rawRdd.map{line=>val field=line.split("\\|");field(1)}
//    content.registerTempTable("SpaceColumn")///////

    sqlContext.sql("select year,country,genre from SpaceColumn limit 1000").foreach(println)


  }

}
