package com.ctvit.learn

import java.text.SimpleDateFormat
import java.util
import java.util.Calendar

/**
 * Created by BaiLu on 2015/3/18.
 */
object DateParse {
  def main(args: Array[String]) {

    val df = new SimpleDateFormat("yyyyMMdd")
    val calendar1 = Calendar.getInstance()
    calendar1.add(Calendar.DAY_OF_WEEK, -1)
    val calendar2 = Calendar.getInstance()
    calendar2.add(Calendar.DAY_OF_WEEK, -2)
    val calendar3 = Calendar.getInstance()
    calendar3.add(Calendar.DAY_OF_WEEK, -3)
    val calendar4 = Calendar.getInstance()
    calendar4.add(Calendar.DAY_OF_WEEK, -4)
    val calendar5 = Calendar.getInstance()
    calendar5.add(Calendar.DAY_OF_WEEK, -5)
    val calendar6 = Calendar.getInstance()
    calendar6.add(Calendar.DAY_OF_WEEK, -6)
    val calendar7 = Calendar.getInstance()
    calendar7.add(Calendar.DAY_OF_WEEK, -7)
    val dates = s"${df.format(calendar1.getTime)},${df.format(calendar2.getTime)},${df.format(calendar3.getTime)},${df.format(calendar4.getTime)},${df.format(calendar5.getTime)},${df.format(calendar6.getTime)},${df.format(calendar7.getTime)}"
    println(dates.toString)
    println(timeSpan())
  }

  def timeSpan(): String = {
    val df = new SimpleDateFormat("yyyyMMdd")
    val buffer=new StringBuffer()
    for (i <- 1 to 10) {
      val calendar = Calendar.getInstance()
      calendar.add(Calendar.DAY_OF_WEEK, -i)
      buffer.append(df.format(calendar.getTime)+",")
    }
    println(buffer)
    val dates=List(buffer)
    for(date<-dates)
      println(date)
    println("#######")
    dates.mkString(",")
    buffer.toString

  }
}
