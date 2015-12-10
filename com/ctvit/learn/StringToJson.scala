package com.ctvit.learn

import java.util

import net.sf.json.JSONObject
import net.sf.json.JSONArray

import scala.collection.mutable.ArrayBuffer

/**
 * Created by BaiLu on 2015/8/10.
 */
object StringToJson {
  def main(args:Array[String]): Unit ={

    val channelMap=new util.HashMap[String,util.ArrayList[String]]()
    val channelList=new util.ArrayList[String]()
    channelList.add("channelID")
    channelMap.put("rec_livelist",channelList)
    val channelobj=JSONObject.fromObject(channelMap)
    println(channelobj)


    val mediaMap=new util.HashMap[String,util.ArrayList[util.HashMap[String,String]]]()
    val mediaList=new util.ArrayList[util.HashMap[String,String]]()
    val mediaInnerMap=new util.HashMap[String,String]()
    mediaInnerMap.put("key1","value1")
    mediaList.add(mediaInnerMap)
    mediaMap.put("rec_vodlist",mediaList)
    val mediaobj=JSONObject.fromObject(mediaMap)
    println(mediaobj)

    println(channelobj.toString.concat(mediaobj.toString).replace("}{",","))



//    val finalRecMap=new util.HashMap[String,util.ArrayList[String]]()
//    finalRecMap.put("rec_livelist",channelList)
//    finalRecMap.put("rec_vodlist",mediaList)
//    val finalRecObj=JSONObject.fromObject(finalRecMap)
//    println(finalRecObj)


  }

}
