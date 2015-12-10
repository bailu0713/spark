package com.ctvit.learn

import java.sql.{ResultSet, Statement, DriverManager, Connection}
import java.text.SimpleDateFormat
import java.util.Calendar
import org.apache.commons.logging._
/**
 * Created by BaiLu on 2015/3/25.
 */
object Test {

  def main(args: Array[String]): Unit = {

    for (j <- 1 to 10) {
//            j=j+1

      //      ？？？？？？？？？？？？？？？
      //为什么错？？？？
      //      ？？？？？？？？？？？？？？
      var r = 1
    }
    //    println(r)

    var i = 0
    while (i < 10) {
      i = i + 1
    }


    println("weekspan=",weekSpan())
  }

  //  ********************
  //        List排序
  //  *********************
  val charlist = "a" :: "b" :: "e" :: "c" :: "1" :: "3" :: "2" :: "4" :: Nil
  val tuplelist = ("a", 1) ::("b", 3) ::("c", 2) ::("a", 4) ::("b", 5) ::("c", 6) ::("a", 9) ::("b", 8) ::("c", 7) ::("a", 10) ::("b", 13) ::("c", 12) :: Nil
  val res = tuplelist.minBy(_._2)
  val min = tuplelist.sortBy(_._2)

  var i = min.size
  var j = 0
  var rec = ""
  while (j < 10) {
    if (i > 0) {
      println(min(i - 1))
      rec += min(i - 1)._1 + ","
      println(rec)
    }
    i -= 1
    j += 1
  }
  val sortcharlist = charlist.sortWith(_.compareTo(_) > 0)
  println("charlist sort ", sortcharlist)
  //  ********************
  //      List添加元素
  //  *********************
  var test = List("")
  for (i <- 1 to 10) {
    test = test :+ i.toString
    test = test.++(List("1"))
  }
  print("list添加元素",test)

  //  ********************
  //      两个循环条件
  //  *********************
for(i<-0 until 10;j<-1 until 11){
    println(i,j)
  }

  val options=(1 to 5).map(i=> if(i%2==0) println(Some(i)) else println(i))




  def weekSpan(): String = {
    val df = new SimpleDateFormat("yyyyMMdd")
    val buffer = new StringBuffer()

    for (i <- 1 to 7) {
      val calendar = Calendar.getInstance()
      calendar.add(Calendar.DAY_OF_WEEK, -i-10)
      buffer.append(df.format(calendar.getTime) + ",")
    }
    buffer.toString
  }
}
