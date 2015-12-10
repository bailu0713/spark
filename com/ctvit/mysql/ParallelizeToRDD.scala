package com.ctvit.mysql

import java.sql.{DriverManager, Connection}

import org.apache.spark.{SparkContext, SparkConf}

import scala.collection.mutable.{ArrayBuffer, ListBuffer}

/**
 * Created by BaiLu on 2015/4/29.
 */
object ParallelizeToRDD {
  val MYSQL_HOST = "10.3.3.182"
  val MYSQL_PORT = "3306"
  val MYSQL_DB = "ire"
  val MYSQL_DB_USER = "ire"
  val MYSQL_DB_PASSWD = "ZAQ!XSW@CDE#"
  val MYSQL_CONNECT = "jdbc:mysql://" + MYSQL_HOST + ":" + MYSQL_PORT + "/ire"
  val MYSQL_DRIVER = "com.mysql.jdbc.Driver"
  val MYSQL_SQL = "SELECT element_info.title, element_info.id, element_info.directors, element_info.actors,element_info.year, element_info.description, element_info.comment, element_info.country, ire_content_relation.level1Id,ire_content_relation.level2Id,ire_content_relation.level3Id,ire_content_relation.level4Id,ire_content_relation.level5Id,ire_content_relation.level6Id,ire_content_relation.seriesType from ire_content_relation INNER JOIN element_info ON ire_content_relation.contentId = element_info.id;"
  //  val MYSQL_SQL = "SELECT element_info.title, element_info.id, element_info.directors, element_info.actors,element_info.year, element_info.description, element_info.type ,element_info.comment, element_info.country from element_info;"
  var con: Connection = null

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("ParallelizeToRdd").setMaster("local")
    val sc = new SparkContext(conf)

    Class.forName(MYSQL_DRIVER)
    con = DriverManager.getConnection(MYSQL_CONNECT, MYSQL_DB_USER, MYSQL_DB_PASSWD)
    val stat = con.createStatement()
    val res = stat.executeQuery(MYSQL_SQL)
    val listbuffer = new ListBuffer[String]
    //    添加元素采用listbuffer和arraybuffer
    val arraybuffer = new ArrayBuffer[String]()

    while (res.next()) {
      val ress = res.getString(1) + res.getString(2) + res.getString(3) + res.getString(4) + res.getString(5) + res.getString(6) + res.getString(7) + res.getString(8)
      listbuffer.+=(ress)
      //      val reslist = List("title:", res.getString(1), "id:", res.getString(2), "directors:", res.getString(3), "actors:", res.getString(4), "year:", res.getString(5), "description:", res.getString(6), "comment:", res.getString(7), "country:", res.getString(8))
      //      sc.parallelize(Seq(reslist))
    }
    val list = listbuffer.toList
    val rdd = sc.parallelize(Seq(list))
    val tmp = rdd.take(1)
    tmp.foreach(println)

  }

}
