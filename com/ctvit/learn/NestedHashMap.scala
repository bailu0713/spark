package com.ctvit.learn

import java.util

/**
 * Created by BaiLu on 2015/9/8.
 */
object NestedHashMap {

  def main(args: Array[String]) {
    val outerMap = new util.HashMap[String, util.HashMap[String, String]]()
    val innerMap = new util.HashMap[String, String]()
    innerMap.put("innerkey", "value")
    outerMap.put("outerkey", innerMap)
    println(outerMap)

  }

}
