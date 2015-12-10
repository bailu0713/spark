package com.ctvit.contentRec

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.SparkContext._

/**
 * Created by BaiLu on 2015/7/28.
 */
object FeatureExtration {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("contentSimarlity").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val rawRdd = sc.textFile("hdfs://lab01:8020/user/bl/data.txt")
    val elementRdd = rawRdd.map { line => val field = line.split("\\|"); (field(0), field(1), field(2), field(3), field(4))}
      .filter(tup => tup._2 != "").filter(tup => tup._2 != "null")

    /**
     * #######################
     * genre feature extration
     * #######################
     */
    /**
     * genreTuple returns (genre,(cid,genre,videoname,year,country))
     **/
    val genreTuple = elementRdd.keyBy(tup => tup._2)
    val genreCount = elementRdd.count()
    //    (genre,genrecount*1.0/allgenrecount)
    /**
     * genreFeature calculate by (genre,genreFeature).join(genre,(cid,genre,viedoname,year,country))
     * returns (genre,(genreFeature,(cid,genre,viedoname,year,country)))
     * map to (cid,genre,genreFeature,videoname,year,country)
     **/
    val genreFeature = elementRdd.groupBy(tup => tup._2)
      .map(tup => (tup._1, tup._2.size * 1.0 / genreCount))
      .join(genreTuple)
      .map(tup => (tup._2._2._1, tup._2._2._2, tup._2._1, tup._2._2._3, tup._2._2._4, tup._2._2._5))

    /**
     * #######################
     * year feature extration
     * #######################
     */
    /**
     * year is null or year is "" first use 0.0 normalize,then test year frequency
     * return (cid,genre,genreFeature,videoname,yearfeature,country)
     * */
    val yearFeature=genreFeature.map{tup=>if(tup._5.matches("\\d+"))
      (tup._1,tup._2,tup._3,tup._4,yearNormalize(tup._5.toInt),tup._6)
    else
      (tup._1,tup._2,tup._3,tup._4,0.0,tup._6)
    }


    /**
     * #######################
     * country feature extration
     * #######################
     */
    /**
     * country is "" first use 0.0 normalize,then test country frequency
     * return(cid,genre,videoname,genrefeature,yearfeature,countryfeature)
     * */
    val countryCount=yearFeature.filter(tup=>tup._6!="").count()
    val coutrytupleCount=yearFeature.groupBy(tup=>tup._6).map(tup=>(tup._1,tup._2.size))
    val countryFeature=yearFeature.keyBy(tup=>tup._6).join(coutrytupleCount)
      .map(tup=>(tup._2._1._1,tup._2._1._2,tup._2._1._3,tup._2._1._4,tup._2._1._5,tup._2._1._6,tup._2._2))
    .map{tup=>
      if(tup._6=="")
        tup._1+"#"+tup._2+"#"+tup._4+"#"+tup._3+"#"+tup._5+"#"+0.0
      else
        tup._1+"#"+tup._2+"#"+tup._4+"#"+tup._3+"#"+tup._5+"#"+tup._7*1.0/countryCount
    }

    /**
     * save to hdfs
     * */
    val hadoopconf=new Configuration()
    val fs=FileSystem.get(hadoopconf)
    val strPath="hdfs://lab01:8020/user/bl/featureextration"
    if (fs.exists(new Path(strPath)))
      fs.delete(new Path(strPath),true)
    countryFeature.saveAsTextFile(strPath)




  }

  /**
   * year normalize
   * in recent 3 years return 0.9
   * in recent 7 years return 0.7
   * .....
   **/
  def yearNormalize(year: Int): Double = {

    val df = new SimpleDateFormat("yyyy")
    val now_year = df.format(new Date())
    if (now_year.toInt - year >= 0 && now_year.toInt - year < 4)
      return 0.9
    if (now_year.toInt - year >= 4 && now_year.toInt - year < 7)
      return 0.7
    if (now_year.toInt - year >= 7 && now_year.toInt - year < 10)
      0.4
    else
      0.1
  }


}
