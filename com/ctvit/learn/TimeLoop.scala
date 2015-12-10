package com.ctvit.learn

import java.text.SimpleDateFormat
import java.util.{Date, Calendar}

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by BaiLu on 2015/10/9.
 */
object TimeLoop {

  val secondsPerMinute = 60
  val minutesPerHour = 60
  val hoursPerDay = 24
  val dayDuration = 7
  val msPerSecond = 1000
  val totalRecNum = 16


  def main(args: Array[String]) {
    val df = new SimpleDateFormat("yyyyMMdd")
    val db_sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    /**
     * 时间1
     * 获取一周前的日期，转化成yyyyMMdd
     **/
    val canlendar = Calendar.getInstance()
    canlendar.add(Calendar.DAY_OF_YEAR, -7)
    val dbTimeAudienceDay = df.format(canlendar.getTime)

    /**
     * 时间2
     * 获取今天的时间初始yyyy-MM-dd 00:00:00
     **/
    val todayTime = df.format(new Date())
    val dbStartLoop = df.parse(todayTime).getTime
    val startTodayTime = db_sdf.format(dbStartLoop)
    /**
     * 当天0时刻开始时间转换成秒
     **/
    val startTodayTimeSecond = db_sdf.parse(startTodayTime).getTime / msPerSecond

    for (i <- 0 until 1440) {

      val dbTimeLiveLoop = db_sdf.format((startTodayTimeSecond + i * secondsPerMinute) * msPerSecond)
      val dbTimeAudienceLoop = dbTimeLiveLoop.split(" ")(1).replaceAll(":", "").substring(0, 4)
      for (column <- Array("", "电视剧", "电影", "少儿", "综合", "体育", "科教", "财经", "娱乐")) {
        if (column.equals("") ) {
          println(dbTimeAudienceLoop+"_all")
          println(dbTimeLiveLoop)
          println(dbTimeAudienceDay)
        }
        else
        {
          println(dbTimeAudienceLoop+"_"+column)
          println(dbTimeLiveLoop)
          println(dbTimeAudienceDay)
        }
      }
    }
  }
}
