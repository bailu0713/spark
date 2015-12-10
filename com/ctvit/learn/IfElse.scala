package com.ctvit.learn

/**
 * Created by BaiLu on 2015/9/8.
 */
object IfElse {
  def main(args: Array[String]) {
  println(ifelse(1))
  }
  def ifelse(flag:Int): String ={
    val key= if(flag==0) "Topcontentlist_5_" + 1 else "Topcontentlist_5_" + 1 + "_" + 1
    key
  }

}
