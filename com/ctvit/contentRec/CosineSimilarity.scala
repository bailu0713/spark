package com.ctvit.contentRec

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.SparkContext._
/**
 * Created by BaiLu on 2015/7/28.
 */
object CosineSimilarity {

  def main(args:Array[String]): Unit ={

    val conf=new SparkConf().setAppName("CosineSimilarity").setMaster("local[*]")
    val sc=new SparkContext(conf)
    val rawRdd=sc.textFile("hdfs://lab01:8020/user/bl/featureextration/part-*")
    /**
     * rawRdd format=(cid,genre,videoname,genrefeature,yearfeature,countryfeature)
     * */
    val content=rawRdd.map{line=>val field=line.split("#");(field(0),field(1),field(2),field(3),field(4),field(5))}
    val contentGroupBy=content.groupBy(tup=>tup._2)

    /**对Iterable做flatmap并filter其中的数据
      * 并不是对joined._2._1做flatmap，因为他本就是flat的
      * */
    val contentPairs=content.keyBy(tup=>tup._2)
      .join(contentGroupBy,10)
    .flatMap(joined=>joined._2._2.filter(tup=>tup._1<joined._2._1._1)
      .map(tup=>((tup._1,tup._2,tup._3,tup._4,tup._5,tup._6),
      (joined._2._1._1,joined._2._1._2,joined._2._1._3,joined._2._1._4,joined._2._1._5,joined._2._1._6))))


    /**
     * cosine similarity calculate
     * */

    val cosineSimilarity=contentPairs.map(tup=>(tup._1._1,tup._1._3,tup._2._1,tup._2._3,
      (tup._1._4.toDouble*tup._2._4.toDouble+tup._1._5.toDouble*tup._2._5.toDouble+tup._1._6.toDouble*tup._2._6.toDouble)*1.0/(math.sqrt(math.pow(tup._1._4.toDouble,2)+math.pow(tup._1._5.toDouble,2)+math.pow(tup._1._6.toDouble,2))*math.sqrt(math.pow(tup._2._4.toDouble,2)+math.pow(tup._2._5.toDouble,2)+math.pow(tup._2._6.toDouble,2)))))
     /**
     * save to hdfs
     * */
    val hadoopconf=new Configuration()
    val fs=FileSystem.get(hadoopconf)
    val strPath="hdfs://lab01:8020/user/bl/contentsimilar"
    if (fs.exists(new Path(strPath)))
      fs.delete(new Path(strPath),true)
    cosineSimilarity.saveAsTextFile(strPath)
  }


}
