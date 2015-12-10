package com.ctvit.learn

import java.text.SimpleDateFormat
import java.util.Date

/**
 * Created by BaiLu on 2015/7/15.
 */
object year {

  def main(args:Array[String]): Unit ={
    println(year_normalize(2015))
  }


  def year_normalize(year: Int): Double = {

    val df = new SimpleDateFormat("yyyy")
    val now_year = df.format(new Date())
    println(now_year.toInt-year)
    if (now_year.toInt - year >= 0 && now_year.toInt - year < 4)
      return 0.9
    if (now_year.toInt - year >= 4 && now_year.toInt - year < 7)
      return 0.7
    if (now_year.toInt - year >= 7 && now_year.toInt - year < 10)
       0.4
    else
       0.1

  }

}
