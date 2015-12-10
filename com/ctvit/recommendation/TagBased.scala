package com.ctvit.recommendation

import java.sql.{ResultSet, DriverManager}

import com.ctvit.AllConfigs
import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkContext, SparkConf}
import scopt.OptionParser

/**
 * Created by BaiLu on 2015/10/23.
 */
object TagBased {
  val configs = new AllConfigs
  val MYSQL_HOST = configs.BOX_MYSQL_HOST
  val MYSQL_PORT = configs.BOX_MYSQL_PORT
  val MYSQL_DB = configs.BOX_MYSQL_DB
  val MYSQL_DB_USER = configs.BOX_MYSQL_DB_USER
  val MYSQL_DB_PASSWD = configs.BOX_MYSQL_DB_PASSWD

  val MYSQL_CONNECT = "jdbc:mysql://" + MYSQL_HOST + ":" + MYSQL_PORT + "/" + MYSQL_DB
  val MYSQL_DRIVER = "com.mysql.jdbc.Driver"

  case class Params(recNumber: Int = 10)

  def main(args: Array[String]) {

    val parser = new OptionParser[Params]("TagBasedRec") {
      head("this is for tag based recommendation")
      opt[Int]("recnumber")
        .action((x, c) => c.copy(recNumber = x))
    }

  }

  def run(params: Params): Unit = {
    val conf = new SparkConf()
    val sc = new SparkContext(conf)
    val rdd = new JdbcRDD(sc, initMysql, "select MovieID,MovieName,DramaType,DramaTypeID from ottelementinfo where MovieID>=? and MovieID<=?; ", 1, 200000000, 1, extractValues)
    .filter(tup=>tup._4!="113")
    .filter(tup=>tup._4!="")
    .filter(tup=>tup._4!=null)
    .foreach(println)
  }

  def initMysql() = {
    Class.forName(MYSQL_DRIVER)
    DriverManager.getConnection(MYSQL_CONNECT, MYSQL_DB_USER, MYSQL_DB_PASSWD)
  }

  def extractValues(r: ResultSet) = {
    (r.getString(1),r.getString(2),r.getString(3),r.getString(4))
  }

}
