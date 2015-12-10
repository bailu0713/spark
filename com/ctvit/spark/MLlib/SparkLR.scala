package com.ctvit.spark.MLlib

import java.util.Random

import scala.math.exp

import breeze.linalg.{Vector, DenseVector}

import org.apache.spark._

/**
 * Created by BaiLu on 2015/11/17.
 */
object SparkLR {

  val D = 17
  val rand = new Random(42)

  case class DataPoint(x: Vector[Double], y: Double)

  def parsePoint(line: String): DataPoint = {
    val value = line.split(" ")
    val y = value(0).toDouble
    var x = new Array[Double](D)
    var i = 0
    while (i < D) {
      x(i) = value(i + 1).toDouble
      i += 1
    }
    DataPoint(new DenseVector(x), y)
  }

  def main(args: Array[String]) {

    val sparkConf = new SparkConf().setAppName("SparkHdfsLR")
    val inputPath = "/user/bl/mllib/sample_svm_data.txt"
    val sc = new SparkContext(sparkConf)
    val lines = sc.textFile(inputPath)
    val points = lines.map(parsePoint _).cache()
    val ITERATIONS = 10

    // Initialize w to a random value
    var w = DenseVector.fill(D) {
      2 * rand.nextDouble - 1
    }
    println("Initial w: " + w)

    var j = 0
    while (j <= ITERATIONS) {
      println("On iteration " + j)
      //      val gradient = points
      //        .map(p => p.x * (1 / (1 + exp(-p.y * w.dot(p.x))) - 1) * p.y)
      //        .reduce(_ + _)
      //      w -= gradient
    }
    println("Final w: " + w)
    sc.stop()

  }

}
