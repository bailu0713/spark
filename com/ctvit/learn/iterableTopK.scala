package com.ctvit.learn

import java.util
import java.util.{Comparator, Collections}

/**
 * Created by BaiLu on 2015/4/7.
 */
object iterableTopK {
  def main(args: Array[String]): Unit = {
    val iterable = Iterable((1, 3.0), (2, 4.0), (0, 2.0))
    val iterator = Iterator("a", "number", "of", "words")
    val list = new util.ArrayList[String]()
    while (iterator.hasNext) {
      //      println(iterator.next())
      list.add(iterator.next())
      Collections.sort(list)

    }
    println(sortIterable(iterable))
  }

  def sortIterable(iterable: Iterable[(Int, Double)]): String = {
    val iterator = iterable.toIterator
    val list = new util.ArrayList[String]()
    while (iterator.hasNext) {
      val temp = iterator.next()
      list.add(temp._2.toString + "#" + temp._1.toString)
    }
    Collections.sort(list, new Comparator[String] {
      override def compare(str1: String, str2: String): Int = str2.compareTo(str1)
    })
    var rec = ""
    var i = 0
    while (i < list.size()) {
      if (i < 10) {
        rec = rec + list.get(i).toString.split("#")(1) + ","
        i += 1
        println(rec)
      }
    }
  rec
  }

  def sortByTopK(iterable:Iterable[(Int,Double)],K:Int): Unit ={
    val list=iterable.toList
    val str=list.sortBy(_._2)
    var j=0
    var i=str.size
    var rec=""
    while(j<K){
      if(i>0){
        rec+=str(i-1)._1.toString+","
      }
      j+=1
      i-=1
    }
  }

}
