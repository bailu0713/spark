package com.ctvit.feature

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.SparkContext._

/**
 * Created by BaiLu on 2015/7/23.
 */
object FeatureExtration {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("feature extration")
    val sc = new SparkContext(conf)

    val rawRdd = sc.textFile("hdfs://lab01:8020/user/bl/data.txt")
    val content = rawRdd
      .map { line => val field = line.split("\\|")
      (field(0), field(1), field(2), field(3),
        field(4))
    }
      .groupBy(tup => tup._2)
    val contentPairs = rawRdd.map { line => val field = line.split("\\|")
      (field(0), field(1), field(2), field(3),
        field(4))
    }.keyBy(tup => tup._2)
      .join(content)
      .flatMap(joined => joined._2._2.filter(tup => tup._1 < joined._2._1._1)
      .map(tup => ((tup._1, tup._2, tup._3, tup._4, tup._5),
      (joined._2._1._1, joined._2._1._2, joined._2._1._3, joined._2._1._4, joined._2._1._5))))


    //    val cart=content.cartesian(content).filter(tup=>tup._1._1<tup._2._1)

    val hadoopconf = new Configuration()
    val fs = FileSystem.get(hadoopconf)
    val dstPath = "hdfs://lab01:8020/user/bl/cartesian"

    if (fs.exists(new Path(dstPath)))
      fs.delete(new Path(dstPath), true)

    contentPairs.saveAsTextFile(dstPath)
  }

}
