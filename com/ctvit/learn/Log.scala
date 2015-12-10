package com.ctvit.learn

import java.text.SimpleDateFormat
import java.util.{Date, Random}

import com.ctvit.redis.AllConfigs
/**
 * Created by BaiLu on 2015/7/28.
 */
object Log {

  val config=new AllConfigs
val REDIS_IP=config.REDIS_IP
  def main(args: Array[String]): Unit = {

  println(REDIS_IP)
    val df=new SimpleDateFormat("yyyyMMdd")
    println(df.format(new Date()))


  }
}
