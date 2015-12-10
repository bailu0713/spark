package com.ctvit.spark.MLlib

import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by BaiLu on 2015/11/17.
 */
object LR {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Logistic Regression")
    val sc = new SparkContext(conf)
    val densedata = sc.textFile("/user/bl/mllib")
    val data = MLUtils.loadLibSVMFile(sc, "/user/bl/mllib/sample_libsvm_data.txt")

    val split = data.randomSplit(Array(1.0), 11)
    val labeledpoint=split(0)
    val arr=labeledpoint.collect()

    arr(0).features.size
    val train = split(0).cache()
    val test = split(1)
    val model = new LogisticRegressionWithLBFGS()
      .setNumClasses(10)
      .run(train)
    val predictandlabels = test.map { case LabeledPoint(label, feature) =>
      val predict = model.predict(feature)
      (label, predict)
    }
    val metrix = new MulticlassMetrics(predictandlabels)
    val precision = metrix.precision

    sc.stop()
  }

}
