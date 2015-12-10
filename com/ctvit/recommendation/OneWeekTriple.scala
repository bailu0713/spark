package com.ctvit.recommendation

import java.text.SimpleDateFormat
import java.util.regex.Pattern
import java.util.Calendar

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.SparkContext._

/**
 * Created by BaiLu on 2015/3/13.
 */
case class Records(userid: String, item: String, duration: String, count: Int, columnid: Int)

object OneWeekRec {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Rec").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext._
    val date = todayDate()
    val dates = weekSpan()

    try {
      //    val sourceRdd=sc.textFile(s"hdfs://192.168.168.162:8020/user/lhq/userrecord/{$date,${date+4}}_*.csv")
      //      val sourceRdd = sc.textFile(s"hdfs://192.168.168.162:8020/user/lhq/userrecord/{([${date+4}-${date}])}_*.csv")

      val recordRdd = sc.textFile(s"hdfs://192.168.168.162:8020/data/ire/userrecord/{$dates}*.csv")
      val collectionRdd = sc.textFile(s"hdfs://192.168.168.162:8020/data/ire/usercollect/{$dates}*.csv")

      //      val recordRdd = sc.textFile(s"hdfs://192.168.168.162:8020/data/ire/userrecord/*.csv")
      //      val collectionRdd = sc.textFile(s"hdfs://192.168.168.162:8020/data/ire/usercollect/*.csv")

      //      #######################
      //      #######SPARK SQL#######
      //      #######################
      //      val rdd = recordRdd.map(_.split(",")).map { field => Records(field(0), field(1), field(2), field(3).toInt, field(4).toInt)}
      //      rdd.registerTempTable("user_record")
      //      val sqlres=sqlContext.sql("select distinct userid,count(*) as counts from user_record group by userid order by counts desc")
      //      sqlres.collect().take(10).foreach(println)


      //      #######################
      //      #######SPARK REGX######
      //      #######################
      //              val rdd=recordRdd.map{line=>val field=line.split(",");field(0)}.filter(_.matches("(?i)[a-z]")).collect().take(20).foreach(println)
      //      val rdd = recordRdd.map { line => val field = line.split(","); (field(0),field(1),field(2).toInt)}.filter{case (user,item,behavior)=>Pattern.compile("(?i)[a-z]").matcher(user).find()==false}


      //      #######################
      //      ######SPARK TRIPLE#####
      //      #######################
      val outputpath = s"hdfs://192.168.168.162:8020/data/ire/weektriple/$date"
      val conf = new Configuration()
      val fs = FileSystem.get(conf)
      if (fs.exists(new Path(outputpath)))
        fs.delete(new Path(outputpath),true)
      val recordRddAgg = recordRdd.filter(line => line.indexOf("NULL") < 0).map { line => val field = line.split(","); ((field(0), field(1)), 1)}
      val collectionRddAgg = collectionRdd.filter(line => line.indexOf("NULL") < 0).map { line => val field = line.split(","); ((field(0), field(1)), 1)}

      //      val recordRddAgg = recordRdd.map { line => val field = line.split(",");((field(0), field(1)), 1)}
      //      val collectionRddAgg = collectionRdd.map { line => val field = line.split(","); ((field(0), field(1)), 1)}
      val finalrdd = recordRddAgg.union(collectionRddAgg).reduceByKey(_ + _)
      //      finalrdd.map { case ((user, item), count) => val score = scorefun(count); user + "," + item + "," + (score / 20 + 1) + "," + score.toDouble}.saveAsTextFile(outputpath)
      finalrdd.map { case ((user, item), count) => val score = scorefun(count); user + "," + item + "," + (score / 20 + 1)}.saveAsTextFile(outputpath)


    } catch {
      case e: Exception => println("$$$$$$######!!!!!!the exception is!!!!!!######$$$$$$" + e)
    }
  }

  def todayDate(): Int = {
    val dateformat = new SimpleDateFormat("yyyyMMdd")
    val today = dateformat.format(Calendar.getInstance().getTime)
    today.toInt
  }

  def timeSpan(): String = {
    val df = new SimpleDateFormat("yyyyMMdd")
    val calendar1 = Calendar.getInstance()
    calendar1.add(Calendar.DAY_OF_WEEK, -1)
    val calendar2 = Calendar.getInstance()
    calendar2.add(Calendar.DAY_OF_WEEK, -2)
    val calendar3 = Calendar.getInstance()
    calendar3.add(Calendar.DAY_OF_WEEK, -3)
    val calendar4 = Calendar.getInstance()
    calendar4.add(Calendar.DAY_OF_WEEK, -4)
    val calendar5 = Calendar.getInstance()
    calendar5.add(Calendar.DAY_OF_WEEK, -5)
    val calendar6 = Calendar.getInstance()
    calendar6.add(Calendar.DAY_OF_WEEK, -6)
    val calendar7 = Calendar.getInstance()
    calendar7.add(Calendar.DAY_OF_WEEK, -7)
    val dates = s"${df.format(calendar1.getTime)},${df.format(calendar2.getTime)},${df.format(calendar3.getTime)},${df.format(calendar4.getTime)},${df.format(calendar5.getTime)},${df.format(calendar6.getTime)},${df.format(calendar7.getTime)}"
    dates.toString
  }

  def weekSpan(): String = {
    val df = new SimpleDateFormat("yyyyMMdd")
    val buffer = new StringBuffer()

    for (i <- 1 to 7) {
      val calendar = Calendar.getInstance()
      calendar.add(Calendar.DAY_OF_WEEK, -i)
      buffer.append(df.format(calendar.getTime) + ",")
    }
    buffer.toString
  }

  def year(): String = {
    val df = new SimpleDateFormat("yyyy")
    val year = df.format(Calendar.getInstance().getTime)
    year.toString
  }

  def scorefun(count: Int): Int = count match {
    case 1 => 10
    case 2 => 10
    case 3 => 20
    case 4 => 20
    case 5 => 30
    case 6 => 30
    case 7 => 40
    case 8 => 40
    case _ => 50
  }

}
