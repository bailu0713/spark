package com.ctvit.spark.MLlib

// General purpose library

import org.apache.spark.{SparkContext, SparkConf}

import scala.xml._

// Spark data manipulation libraries

import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

// Spark machine learning libraries

import org.apache.spark.ml.feature.{HashingTF, Tokenizer}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.ml.Pipeline

/**
 * Created by BaiLu on 2015/12/10.
 */
object BinaryClassfication {

  def main(args: Array[String]) {
    val conf = new SparkConf()
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val fileName = "Posts.small.xml"
    val textFile = sc.textFile(fileName)
    val postsXml = textFile.map(_.trim)
      .filter(!_.startsWith("<?xml version="))
      .filter(_ != "<posts>")
      .filter(_ != "</posts>")


    val postsRDD = postsXml.map { s =>
      val xml = XML.loadString(s)
      val id = (xml \ "@Id").text
      val tags = (xml \ "@Tags").text
      val title = (xml \ "@Title").text
      val body = (xml \ "@Body").text
      val bodyPlain = ("<\\S+>".r).replaceAllIn(body, " ")
      val text = (title + " " + bodyPlain).replaceAll("\n", " ").replaceAll("( )+", " ")
      Row(id, tags, text)
    }


    val schemaString = "Id Tags Text"
    val schema = StructType(schemaString.split(" ").map(fieldName =>StructField(fieldName, StringType, true)))
    val postsDf = sqlContext.createDataFrame(postsRDD, schema)


    val targetTag = "java"
    val myudf = (str: String) => {
      if (str.contains(targetTag)) 1.0 else 0.0
    }
    def myudfs(str:String): Double ={
      if (str.contains(targetTag)) 1.0 else 0.0
    }
/*
    val sqlfunc = udf(myudf)
    val postsLabeled = postsDf.withColumn("Label", sqlfunc(col("Tags")))
    val positive = postsLabeled.filter(postsDf("Label") > 0.0)
    val negative = postsLabeled.filter(postsDf("Label ")< 1.0)
    val positiveTrain = positive.sample(false, 0.9)
    val negativeTrain = negative.sample(false, 0.9)
    val training = positiveTrain.unionAll(negativeTrain)


    val negativeTrainTmp = negativeTrain
      .withColumnRenamed("Label", "Flag")
      .select("Id", "Flag")
    val negativeTest = negative
      .join(negativeTrainTmp,negative("Id") === negativeTrainTmp("Id"),"LeftOuter")
      .filter("Flag is null")
      .select(negative("Id").toString(), "Tags", "Text", "Label")
    val positiveTrainTmp = positiveTrain
      .withColumnRenamed("Label", "Flag")
      .select("Id", "Flag")
    val positiveTest = positive
      .join(positiveTrainTmp,positive("Id") === positiveTrainTmp("Id"),"LeftOuter")
      .filter("Flag is null")
      .select(positive("Id").toString(), "Tags", "Text", "Label")
    val testing = negativeTest
      .unionAll(positiveTest)


    //train model
    val numFeatures = 64000
    val numEpochs = 30
    val regParam = 0.02
    val tokenizer = new Tokenizer()
      .setInputCol("Text")
      .setOutputCol("Words")
    val hashingTF = new HashingTF()
      .setNumFeatures(numFeatures)
      .setInputCol(tokenizer.getOutputCol)
      .setOutputCol("Features")
    val lr = new LogisticRegression()
      .setMaxIter(numEpochs)
      .setRegParam(regParam)
      .setFeaturesCol ("Features")
      .setLabelCol("Label")
      .setRawPredictionCol("Score")
      .setPredictionCol("Prediction")
    val pipeline = new Pipeline()
      .setStages(Array(tokenizer, hashingTF, lr))
    val model = pipeline.fit(training)


    //test
    val testTitle =
      "Easiest way to merge a release into one JAR file"
    val testBoby =
      """Is there a tool or script which easily merges a bunch
 of href="http://en.wikipedia.org/wiki/JAR_%28file_format
 %29" JAR files into one JAR file? A bonus would be to
 easily set the main-file manifest and make it executable.
 I would like to run it with something like: As far as I
 can tell, it has no dependencies which indicates that it
 shouldn't be an easy single-file tool, but the downloaded
 ZIP file contains a lot of libraries."""

    val testText = testTitle + testBoby
    val testDF = sqlContext
      .createDataFrame(Seq((99.0, testText)))
      .toDF("Label", "Text")
    val result = model.transform(testDF)
    val prediction = result
      .collect()(0)(6)
      .asInstanceOf[Double]
    print("Prediction: " + prediction)


    val testingResult = model.transform(testing)
    val testingResultScores = testingResult
      .select("Prediction", "Label")
      .rdd
      .map(r => (r(0).asInstanceOf[Double], r(1)
      .asInstanceOf[Double]))
    val bc = new BinaryClassificationMetrics(testingResultScores)
    val roc = bc.areaUnderROC
    print("Area under the ROC:" + roc)
    */
  }

}
