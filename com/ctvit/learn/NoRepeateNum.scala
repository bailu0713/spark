package com.ctvit.learn

import scala.collection.mutable.ArrayBuffer

/**
 * Created by BaiLu on 2015/12/1.
 */
object NoRepeateNum {
  val random = new util.Random()

  def main(args: Array[String]) {
    val arr = getNoRepeate(20)
    for (i <- 0 until 20) {
      println(arr(i))
    }
  }

  def getNoRepeate(n: Int): Array[Int] = {

    val sequence = new Array[Int](n)
    val output = new Array[Int](n)
    for (j <- 0 until n) {
      sequence(j) = j
    }
    var end = n - 1
    var i = 0
    while (i < n) {
      val index = random.nextInt(end + 1)
      output(i) = sequence(index)
      sequence(index) = sequence(end)
      i += 1
      end -= 1
    }
    output
  }
}
