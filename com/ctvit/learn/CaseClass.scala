package com.ctvit.learn

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

/**
 * Created by BaiLu on 2015/3/12.
 */

object CaseClass {

  case class Record(key: String, value: String)
  case class Params(
  param1:String,
  param2:String,
  param3:String
                     )

  def main(args: Array[String]): Unit = {

    val date = Calendar.getInstance().getTime
    val dates = new SimpleDateFormat("yyyyMMdd")
    val today = dates.format(date)
    //    val day=dates.parse("2015-03-06")
    println(today)
    println(timedelta(Calendar.getInstance().getTime))


  }

  def timedelta(date: Date): String = {
    val dateformat = new SimpleDateFormat("yyyyMMdd")
    val today = dateformat.format(date)
    (today.toInt - 7).toString
  }

}