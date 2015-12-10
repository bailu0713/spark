package com.ctvit.recommendation

import java.io.FileInputStream
import java.util.Properties


/**
 * Created by BaiLu on 2015/8/19.
 */
object ReadProperty {

  def main(args: Array[String]) {
    val inputstream=new FileInputStream("E:\\config.property")
    val properties=new Properties()
    properties.load(inputstream)
    /**
     * ALS算法参数设置
     **/
    val ALS_ITERATION = properties.getProperty("ALS_ITERATION").toInt
    val ALS_RANK = properties.getProperty("ALS_RANK").toInt
    val ALS_LAMBDA = properties.getProperty("ALS_LAMBDA").toDouble
    val ALS_BLOCK = properties.getProperty("ALS_BLOCK").toInt
    /**
     * mysql配置信息
     **/
    val MYSQL_HOST = properties.getProperty("MYSQL_HOST")
    val MYSQL_PORT = properties.getProperty("MYSQL_PORT")
    val MYSQL_DB = properties.getProperty("MYSQL_DB")
    val MYSQL_DB_USER = properties.getProperty("MYSQL_DB_USER")
    val MYSQL_DB_PASSWD = properties.getProperty("MYSQL_DB_PASSWD")
    val MYSQL_CONNECT =properties.getProperty("MYSQL_CONNECT")
    //  val MYSQL_CONNECT = "jdbc:mysql://" + MYSQL_HOST + ":" + MYSQL_PORT + "/" + MYSQL_DB
    val MYSQL_DRIVER = properties.getProperty("MYSQL_DRIVER")
    println(MYSQL_DRIVER)
    val MYSQL_QUERY = properties.getProperty("MYSQL_QUERY")
    /**
     * redis配置信息
     **/
    val REDIS_IP = properties.getProperty("REDIS_IP")
    val REDIS_PORT = properties.getProperty("REDIS_PORT").toInt
  }

}
