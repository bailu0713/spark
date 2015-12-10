package com.ctvit.learn

/**
 * Created by BaiLu on 2015/4/3.
 */

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.rdd.JdbcRDD
import java.sql.{Connection, DriverManager, ResultSet}

object ConnectMysql {

  def main(args:Array[String]): Unit ={

    val conf=new SparkConf().setAppName("ConnectMySql")
    val sc=new SparkContext(conf)

    val url="jdbc:mysql://localhost:3306/hadoopdb"
    val username = "hduser"
    val password = "******"
    Class.forName("com.mysql.jdbc.Driver").newInstance


//    JDBC类的定义和参数
//    class JdbcRDD[T](sc : SparkContext, getConnection : scala.Function0[java.sql.Connection], sql :String, lowerBound : Long, upperBound :Long, numPartitions : Int, mapRow : scala.Function1[java.sql.ResultSet, T] = { /* compiled code */ })

//    val myRDD = new JdbcRDD( sc, () =>DriverManager.getConnection(url,username,password) ,"select first_name,last_name,gender from person limit ?, ?",1, 5, 2, r => r.getString("last_name") + ", " + r.getString("first_name"))

    val myRDD = new JdbcRDD( sc, () =>DriverManager.getConnection(url,username,password) ,"insert into table status values('ture','run')",1, 1, 2)
//    myRDD.foreach(println)
//    myRDD.saveAsTextFile("person")

//    JdbcRDD( SparkContext,
//      getConnection: () => Connection,
//      sql: String,
//      lowerBound: Long,
//      upperBound: Long,
//      numPartitions: Int,
//      mapRow: (ResultSet) => T = JdbcRDD.resultSetToObjectArray
//    )
  }

}
