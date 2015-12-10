package com.ctvit.sql

import java.text.SimpleDateFormat
import java.util.Date
import java.util.regex.Pattern

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.SparkContext._
import org.apache.spark.sql._

/**
 * Created by BaiLu on 2015/7/14.
 */
object ContentVector {


  case class element_vector(contentId: String, contentName: String, contentYear: String, contentCountry: String)
  case class CidCount(contentId:String,hotCount:Int)
  def main(args: Array[String]): Unit = {
    val startTime = System.currentTimeMillis()

    val conf = new SparkConf().setAppName("ContentVector").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext._

    val element_info_rdd = sc.textFile("hdfs://lab01:8020/user/zyl/sql/element_info.csv")
    val element_info_yearcount = element_info_rdd.filter {
      _.split(",")(25).matches("\\d+")
    }.count()

    //    0:contentid,1:contentName,18:contentyear,19:contentCountry
    val year_vector = element_info_rdd.filter {
      _.split(",")(25) != ""
    }.filter {
      _.split(",")(25) != "无"
    }.filter {
      _.split(",")(25).matches("\\d+")
    }.map { line => val field = line.split(","); (field(0), field(1), year_normalize(field(25).toInt), field(26))}
    //
    val tempTable = year_vector.filter { tup => tup._4 != ""}.map(tup => element_vector(tup._1, tup._2, tup._3.toString, tup._4))
//    tempTable.registerTempTable("element_vector")

    //    val country_vectory=sqlContext.sql("select contentCountry,count(*) from element_vector group by contentCountry").map{case Row(contentCoutry:String,countryCount:String)=>(contentCoutry,countryCount.toInt/element_info_yearcount.toInt)}

    //initialize view count
    val country_vectory = sqlContext.sql("select contentCountry,count(*) from element_vector group by contentCountry").map { row => (row(0).toString, row(1).toString.toInt * 1.0 / element_info_yearcount.toInt)}


    //save the vector ()
    val year_country_vector = year_vector.keyBy(tup => tup._4).join(country_vectory.keyBy(tup => tup._1)).map(tup => (tup._2._1._1, (tup._2._1._2, tup._2._1._3, tup._2._2._2)))


    // calculate the movie hot
    val hotRdd=sc.textFile("hdfs://lab01:8020/user/zyl/vsp/vod/*.csv")
    val cidHot=hotRdd.map{line=>val field=line.split(",");(field(5),1)}.reduceByKey(_+_).map(tup=>CidCount(tup._1.toString,tup._2.toInt))
    val logCount=hotRdd.count()
//    cidHot.registerTempTable("cidcount")///////
    val sqlHotRes=sqlContext.sql("select contentId,hotCount from cidcount").map(row=>(row(0).toString,hot_normalize(row(1).toString.toInt*1.0/logCount))).join(year_country_vector,20).map(tup=>(tup._1,tup._2._2._1,tup._2._2._2,tup._2._2._3,tup._2._1))

    //(contentid,contentname,yearvector,countryvector,hotvector)
    val strpath = "hdfs://lab01:8020/user/bl/year_country_vector"
    val dstPath = new Path(strpath)
    val hadoopconf = new Configuration()
    val fs = FileSystem.get(hadoopconf)
    if (fs.exists(dstPath))
      fs.delete(dstPath, true)
    sqlHotRes.saveAsTextFile(strpath)


    val endTime = (System.currentTimeMillis() - startTime) / 1000
    println("### runtime= ", endTime, " seconds !###")
  }

  def year_normalize(year: Int): Double = {

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

  def hot_normalize(hotBulk: Double): Double = {

    val df = new SimpleDateFormat("yyyy")
    val now_year = df.format(new Date())
    if (now_year.toInt - hotBulk > 0.8 && now_year.toInt - hotBulk <= 1)
      return 0.9
    if (now_year.toInt - hotBulk >0.5 && now_year.toInt - hotBulk < 0.8)
      return 0.7
    if (now_year.toInt - hotBulk >0.3 && now_year.toInt - hotBulk < 0.5)
      0.4
    else
      0.1

  }


}
