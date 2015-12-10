package com.ctvit.learn

/**
 * Created by BaiLu on 2015/4/3.
 */

import java.sql.{PreparedStatement, Connection, DriverManager}

import com.mysql.jdbc.Driver
import org.apache.spark.{SparkContext, SparkConf}

object SparkMySQLIntegration {

  case class Person(name: String, age: Int)

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("SparkRDDCount").setMaster("local")
    val sc = new SparkContext(conf)
    val data = sc.parallelize(List(("Tom", 31), ("Jack", 22), ("Mary", 25)))
    def func(iter: Iterator[(String, Int)]): Unit = {
      //      Class.forName("com.mysql.jdbc.Driver ")
      var conn: Connection = null
      val d: Driver = null
      var pstmt: PreparedStatement = null
      try {
        val url = "jdbc:mysql://localhost:3306/person"
        val user = "root"
        val password = ""
        //在forPartition函数内打开连接，这样连接将在worker上打开
        conn = DriverManager.getConnection(url, user, password)
        while (iter.hasNext) {
          val item = iter.next()
          println(item._1 + "," + item._2)
          val sql = "insert into TBL_PERSON(name, age) values (?, ?)"
          pstmt = conn.prepareStatement(sql)
          pstmt.setString(1, item._1)
          pstmt.setInt(2, item._2)
          pstmt.executeUpdate()
        }
      } catch {
        case e: Exception => e.printStackTrace()
      } finally {
        if (pstmt != null) {
          pstmt.close()
        }
        if (conn != null) {
          conn.close()
        }
      }
    }
    data.foreachPartition(func)
  }

}
