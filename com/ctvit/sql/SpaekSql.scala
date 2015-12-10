package com.ctvit.sql

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by BaiLu on 2015/7/13.
 */
object SpaekSql {
  def main(args:Array[String]): Unit ={

    val conf=new SparkConf().setMaster("local").setAppName("SQL")
    val sc=new SparkContext(conf)
    val sqlContext=new SQLContext(sc)
    import sqlContext._
    val df=sqlContext

  }

}
