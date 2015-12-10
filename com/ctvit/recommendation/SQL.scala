package com.ctvit.recommendation

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql._

/**
 * Created by BaiLu on 2015/3/12.
 */
case class Rec(key: String, value: String)

object SQL {
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local").setAppName("SQL")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext._
    import sqlContext.implicits._
//    val record = sc.textFile("hdfs://192.168.168.41:8020/user/zyl/vsp/vod/20150605.csv").map(_.split(",")).map(recs => Rec(recs(0), s"val_${recs(1)}"))
    val record = sc.textFile("hdfs://192.168.168.41:8020/user/zyl/vsp/vod/20150605.csv").map(_.split(",")).map(recs => Rec(recs(0), recs(1))).toDF()
    val records = sc.textFile("hdfs://192.168.168.41:8020/user/zyl/vsp/vod/20150605.csv").map(_.split(",")).map(recs => Rec(recs(0),recs(1))).toDF()
    record.registerTempTable("record")///////
    records.registerTempTable("records")//////

    val res = sqlContext.sql("select count(*) from (select key,count(*) from record group by key)a")
    val ress=sqlContext.sql("select * from (select key,value from record limit 100)a join (select key,value from records limit 100)b on a.key=b.key")
    res.collect().take(10).foreach(println)
    ress.collect().take(10).foreach(println)
//    ress.partitions(10)
    println(ress.count())
  }
}
