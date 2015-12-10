package com.ctvit.spark.examples

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by BaiLu on 2015/9/30.
 */
object WordCount {

  def main(args: Array[String]) {
    val inputPath: String = "hdfs://nameservice1/user/zhongshi/data/random_submission.csv"
    val outputPath: String = "hdfs://nameservice1/user/zhongshi/spark"

    val hadoopconf = new Configuration()
    val fs: FileSystem = FileSystem.get(hadoopconf)
    if (fs.exists(new Path(outputPath))) {
      fs.delete(new Path(outputPath), true)
    }
    val conf = new SparkConf()
    val sc = new SparkContext(conf)
    val wc = sc.textFile(inputPath)
      .flatMap(_.split(","))

      .map(word => (word, 1))
      .reduceByKey(_ + _, 15)
      .saveAsTextFile(outputPath)
  }

}
