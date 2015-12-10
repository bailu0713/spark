package com.ctvit.spark.MLlib

import java.io.FileInputStream
import java.util.Properties

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by BaiLu on 2015/8/20.
 */
object GetProperty {
  def main(args: Array[String]) {
        val inputStream = new FileInputStream("/root/jar/bl/config.property")
//    val inputStream = this.getClass.getClassLoader.getResourceAsStream("config.property")
    if (inputStream == null) {
      println("get null data")
      return
    }
    val property = new Properties()
    property.load(inputStream)
    val IP = property.getProperty("TIME_SPAN")
    println(IP)
    val PORT = property.getProperty("ALS_RANK")
    println(PORT)
    val conf = new SparkConf().setAppName("getProperty").setMaster("local[*]")
    val sc = new SparkContext(conf)
    inputStream.close()
    sc.stop()
  }

}
