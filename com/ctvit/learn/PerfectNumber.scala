package com.ctvit.learn

import java.util.regex.Pattern

/**
 * Created by BaiLu on 2015/7/20.
 */
object PerfectNumber {

  def main(args: Array[String]): Unit = {
      println(sumOfFactors(100))
    println("Some(1000403702)".matches("\\d+"))
    val regx="[^0-9]"
    val pattern=Pattern.compile(regx)
    println(pattern.matcher("Some(1000403702").replaceAll(""))
  }

  def sumOfFactors(number: Int) = {
    (1 /: (2 until number)) { (sum, i) => if (sum % i == 0) sum + i else sum}
  }

}
