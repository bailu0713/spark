package com.ctvit

/**
 * Created by BaiLu on 2015/11/19.
 */
object LearnSimpleTest {
  def main(args: Array[String]) {
    println(casePercent(0.5689))
  }
  def casePercent(percent: Double): Double = {
    if (percent >= 0.0 && percent <= 0.1)
      0.1
    else if (percent > 0.1 && percent <= 0.2)
      0.2
    else if (percent > 0.2 && percent <= 0.3)
      0.3
    else if (percent > 0.3 && percent <= 0.4)
      0.4
    else if (percent > 0.4 && percent <= 0.5)
      0.5
    else if (percent > 0.5 && percent <= 0.6)
      0.6
    else if (percent > 0.6 && percent <= 0.7)
      0.7
    else if (percent > 0.7 && percent <= 0.8)
      0.8
    else if (percent > 0.8 && percent <= 0.9)
      0.9
    else
      1.0
  }


}
