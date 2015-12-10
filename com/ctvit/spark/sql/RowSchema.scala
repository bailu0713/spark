package com.ctvit.spark.sql

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by BaiLu on 2015/12/10.
 */
object RowSchema {
  def main(args: Array[String]) {
    val conf=new SparkConf()
    val sc=new SparkContext(conf)
    val sqlContext=new SQLContext(sc)
    val rdd=sc.textFile("hdfs://lab01:8020/user/bl/movielens/ml-1m/ratings.dat")
    val row=rdd.map(line=>line.split("::")).map(arr=>Row(arr(0),arr(1),arr(2)))
    val schemaString="user item rating"
    val schema=StructType(schemaString.split(" ").map(field=>StructField(field,StringType,true)))
    val dataFrame=sqlContext.createDataFrame(row,schema)
    dataFrame.registerTempTable("movielens")
//    sqlContext.sql("select * from movielens limit 100").foreach(println)
    dataFrame
    .select("user","item","rating")
    .filter("rating>4")
    .limit(100)
    .foreach(println)
    sc.stop()
  }

}
