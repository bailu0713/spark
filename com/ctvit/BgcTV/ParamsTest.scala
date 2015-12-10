package com.ctvit.BgcTV

/**
 * Created by BaiLu on 2015/9/9.
 */

import java.sql.{Connection, DriverManager}
import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.{SparkConf, SparkContext}
import scopt.OptionParser

object ParamsTest {

  private case class Params(
                             numIterations: Int = 10,
                             lambda: Double = 1.0,
                             rank: Int = 10,
                             numBlocks: Int = 5,
                             recNumber: Int = 5,
                             timeSpan: Int = 2
                             )

  /**
   * mysql配置信息
   **/
  val MYSQL_HOST = "172.16.168.57"
  val MYSQL_PORT = "3306"
  val MYSQL_DB = "ire"
  val MYSQL_DB_USER = "ire"
  val MYSQL_DB_PASSWD = "ZAQ!XSW@CDE#"
  val MYSQL_CONNECT = "jdbc:mysql://" + MYSQL_HOST + ":" + MYSQL_PORT + "/" + MYSQL_DB
  val MYSQL_DRIVER = "com.mysql.jdbc.Driver"

  def main(args: Array[String]) {
    val defaultParams = Params()
    val startTime = System.nanoTime()
    val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val parser = new OptionParser[Params]("BehaviorRecParams") {
      head("BehaviorRecProduct: an example Recommendation app for plain text data. ")
      opt[Int]("numIterations")
        .text(s"number of iterations, default: ${defaultParams.numIterations}")
        .action((x, c) => c.copy(numIterations = x))
      opt[Double]("lambda")
        .text(s"number of lambda, default: ${defaultParams.lambda}")
        .action((x, c) => c.copy(lambda = x))
      opt[Int]("rank")
        .text(s"number of lambda, default: ${defaultParams.rank}")
        .action((x, c) => c.copy(rank = x))
      opt[Int]("numBlocks")
        .text(s"number of lambda, default: ${defaultParams.numBlocks}")
        .action((x, c) => c.copy(numBlocks = x))
      opt[Int]("recNumber")
        .text(s"number of lambda, default: ${defaultParams.recNumber}")
        .action((x, c) => c.copy(recNumber = x))
      opt[Int]("timeSpan")
        .text(s"number of lambda, default: ${defaultParams.timeSpan}")
        .action((x, c) => c.copy(timeSpan = x))
    }
    parser.parse(args, defaultParams).map { params =>
      run(params)
    }.getOrElse {
      parser.showUsageAsError
      sys.exit(1)
    }
  }

  private def run(params: Params) {
    val conf = new SparkConf().setAppName("BehaviorRecommendaton")
    val sc = new SparkContext(conf)

    /**
     * 读取配置文件信息，之前的参数采用配置文件的方式获取
     **/
    val timespan = timeSpans(params.timeSpan)
    val HDFS_DIR = s"hdfs://172.16.141.215:8020/data/ire/source/rec/xor/vod/{$timespan}.csv"
    val rawRdd = sc.textFile(HDFS_DIR)
    val tripleRdd = rawRdd.map { line => val field = line.split(","); (field(11), field(0))}
      .filter(tup => tup._1.matches("\\d+"))
      .filter(tup => tup._2.matches("\\d+"))
      .map { tup =>
      ((tup._1, tup._2), 1)
    }.reduceByKey(_ + _)

//    val tripleRating = tripleRdd.map(tup => Rating(tup._1._1.toInt, tup._1._2.toInt, tup._2.toDouble))
//    val model = new ALS()
//      .setIterations(params.numIterations)
//      .setRank(params.rank)
//      .setLambda(params.lambda)
//      .setBlocks(params.numBlocks)
//      .run(tripleRating)
//    val recRdd = model.predict(tripleRdd.map(tup => (tup._1._1.toInt, tup._1._2.toInt)))
//      .map { case Rating(user, item, score) => (user, (item, score))}
    tripleRdd.take(1000).foreach(println)
    sc.stop()

  }

  /**
   * 设置读取数据文件的时间间隔,用时间天作为间隔参数
   **/
  def timeSpans(span: Int): String = {
    val df = new SimpleDateFormat("yyyyMMdd")
    val buffer = new StringBuffer()
    for (i <- 1 to span) {
      val canlendar = Calendar.getInstance()
      canlendar.add(Calendar.DAY_OF_YEAR, -i)
      buffer.append(df.format(canlendar.getTime) + ",")
    }
    buffer.toString
  }

  def initMySQL(): Connection = {
    Class.forName(MYSQL_DRIVER)
    DriverManager.getConnection(MYSQL_CONNECT, MYSQL_DB_USER, MYSQL_DB_PASSWD)
  }

}
