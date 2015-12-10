package com.ctvit.tianchi

import java.io._
import java.sql.{Statement, DriverManager, Connection}
import java.util
import java.util.{Comparator, Collections}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._


case class Records(user_id: String, item_id: String, rating: Double)

object Triple {
  def main(args: Array[String]): Unit = {
    val mysqlOpera = new MysqlOperation
    val sc = new SparkContext()
    val conf = new SparkConf().setAppName("TianChiRec")

    val configuration = new Configuration()
    val fs = FileSystem.get(configuration)
    val outputPath = "hdfs://lab01:8020/user/bl/data/tianchi/recTop10"
    if (fs.exists(new Path(outputPath))) {
      fs.delete(new Path(outputPath),true)
    }

    val sqlContext = new SQLContext(sc)
    import sqlContext._

    val rawRdd = sc.textFile("hdfs://192.168.168.162:8020/user/bl/data/tianchi/tianchi_mobile_recommend_train_user.csv")
    //    val rawRdd = sc.textFile("hdfs://192.168.168.162:8020/user/bl/data/iterableSortTest.csv")
    //    val clickTriple = rawRdd.map{line=>val field=line.split(",");(field(0),field(1),field(2))}.filter{case (user,item,behavior)=>behavior.toInt==1}.map{case(user,item,behavior)=>((user,item),1)}.reduceByKey(_+_).filter{case((user,item),count)=>count<3}
    val clickTriple = rawRdd.map { line => val field = line.split(","); (field(0), field(1), field(2))}.filter { case (user, item, behvaior) => behvaior.toInt < 3}.map { case (user, item, behavior) => ((user, item), 1)}.reduceByKey(_ + _).filter { case ((user, item), count) => count > 15}.map { case ((user, item), count) => (user.toInt, item.toInt)}

    val trainTriple = rawRdd.map { line => val field = line.split(","); (field(0), field(1), field(2))}.filter { case (user, item, behavior) => behavior.toInt > 2}.map { case (user, item, behavior) => ((user, item), behavior.toInt + 1)}.reduceByKey(_ + _).map { case ((user, item), count) =>
      if (count > 4)
        Rating(user.toInt, item.toInt, 5.0)
      else
        Rating(user.toInt, item.toInt, 4.0)
    }
    val rank = 10
    val numIterations = 20
    val model = ALS.train(trainTriple, rank, numIterations, 0.01)

//    val implicitmodel=ALS.trainImplicit(trainTriple, rank, numIterations, 0.01,1.0)


//    val modelObj=new ObjectOutputStream(new FileOutputStream("hdfs://lab01:8020/model/recommendation"))
    val modelObj=new ObjectOutputStream(fs.create(new Path("hdfs://lab01:8020/model/recommendation")))
    modelObj.writeObject(model)
    val fis=new ObjectInputStream(new FileInputStream("hdfs://lab01:8020/model/recommendation"))
    val alsModel=fis.readObject().asInstanceOf
    fis.close()


    //topN计算，用groupBy，计算每个item的size
    //    trainTriple.groupBy(tup=>tup._1).map{tup=>(tup._1,tup._2.size)}


    //    公司用格式
    //    val tempRdd = model.predict(clickTriple).map { case Rating(user, item, rating) => (user, (item, rating))}.groupByKey().map {
    //      line =>
    //        val key = line._1
    //        val values = line._2
    //        val score=values.map(value=>value._2)
    //        val item=values.map(value=>value._1)
    //        (key,item,score)
    //    }


    //    val tempRdd = model.predict(clickTriple).map { case Rating(user, item, rating) => (user, item, rating)}.groupBy(tup=>tup._1).sortBy{tup=>tup._1}
    //val tempRdd = model.predict(clickTriple).map { case Rating(user, item, rating) => (user, (item, rating))}.groupByKey()
    //    val tempRdd = model.predict(clickTriple).map { case Rating(user, item, rating) => (user, (item, rating))}.groupByKey().map { case (user, ratings: Iterable[(Int, Double)]) => user + "\t" + sortByTopK(ratings, 10)}

    //        Spark SQL 格式
    //        val tempRdd=model.predict(clickTriple).map{case Rating(user,item,rating)=>Records(user.toString,item.toString,rating)}
    //    比赛用的格式
    val tempRdd = model.predict(clickTriple).map { case Rating(user, item, rating) => (user, (item, rating))}.groupByKey().map { case (user, ratings: Iterable[(Int, Double)]) => (user, sortByTopK(ratings, 10))}.map { tup => tup._1 + "," + tup._2.split(",")(0)}
    //        val tempRdd=model.predict(clickTriple).map{case Rating(user,item,rating)=>(user,item,rating)}.filter{tup=>tup._3>4.998}.sortBy(_._1).map(tup=>tup._1+","+tup._2)
    //    val tempRdd=model.predict(clickTriple).map{case Rating(user,item,rating)=>(user,item,rating)}.sortBy(_._3).map(tup=>tup._1+","+tup._2+","+tup._3)


    //    Spark SQL
    //        tempRdd.registerTempTable("recsys")
    //        val res=sqlContext.sql("select user_id,concat_ws(',',collect_set(item_id)) from(select user_id ,item_id,row_number() over (partition by user_id order by rating desc) as rank from (select user_id,item_id,sum(rating) as rating from train_user group by user_id,item_id )a)b where rank<10 group by user_id;")


    tempRdd.saveAsTextFile(outputPath)

    //    setMysqlFlag()
    mysqlOpera.runSuccess()
  }

  def rules(count: Int): Int = {
    if (count <= 10)
      1
    if (count > 10 && count <= 20)
      2
    if (count > 20 && count <= 30)
      3
    if (30 < count && count <= 40)
      4
    else
      5
  }

  def iterableTopK(iterable: Iterable[(Int, Double)], k: Int): String = {
    val iterator = iterable.toIterator
    val list = new util.ArrayList[String]()
    while (iterator.hasNext) {
      val tmp = iterator.next()
      list.add(tmp._2.toString + "#" + tmp._1.toString)
    }
    //    递减的排序
    Collections.sort(list, new Comparator[String] {
      override def compare(str1: String, str2: String): Int = str2.compareTo(str1)
    })
    var i = 0
    var rec = ""
    while (i < list.size()) {
      if (i < k) {
        rec = rec + list.get(i).toString.split("#")(1) + ","
      }
      i += 1
    }
    rec
  }

  def sortByTopK(iterable: Iterable[(Int, Double)], K: Int): String = {
    val trans = iterable.toList
    val list = trans.sortBy(_._2)
    var j = list.size
    var i = 0
    var rec = ""
    while (i < K) {
      if (j > 0) {
        rec += list(j - 1)._1.toString + ","
      }
      i += 1
      j -= 1
    }
    rec

  }

  def setMysqlFlag(): Unit = {
    val MYSQL_HOST = "10.3.3.182"
    val MYSQL_PORT = "3306"
    val MYSQL_USER = "ire"
    val MYSQL_PASSWORD = "ZAQ!XSW@CDE#"
    val MYSQL_URL = "jdbc:mysql://" + MYSQL_HOST + ":" + MYSQL_PORT + "/ire"
    val MYSQL_NAME = "com.mysql.jdbc.Driver"
    val RUN_SUCCESS_SQL = "update task_state set endFlag='1',endTime=now() where taskId='vsp_behaviorRec' and currentDate=curdate()"
    val RUN_ERROR_SQL = "update task_state set endFlag='0',endTime=now() where taskId='vsp_behaviorRec' and currentDate=curdate()"
    var con: Connection = null

    try {
      Class.forName(MYSQL_NAME)
      con = DriverManager.getConnection(MYSQL_URL, MYSQL_USER, MYSQL_PASSWORD)
      val stat: Statement = con.createStatement
      stat.execute(RUN_SUCCESS_SQL.toString)
    }
    catch {
      case e: FileNotFoundException => {
        e.printStackTrace()
      }
      case e: IOException => {
        e.printStackTrace()
      }
    }
  }

  //mysql操作类，执行成功写入1，执行失败写入0
  class MysqlOperation {

    val MYSQL_HOST = "10.3.3.182"
    val MYSQL_PORT = "3306"
    val MYSQL_USER = "ire"
    val MYSQL_PASSWORD = "ZAQ!XSW@CDE#"
    val MYSQL_URL = "jdbc:mysql://" + MYSQL_HOST + ":" + MYSQL_PORT + "/ire"
    val MYSQL_NAME = "com.mysql.jdbc.Driver"
    val RUN_SUCCESS_SQL = "update task_state set endFlag='1',endTime=now() where taskId='vsp_behaviorRec' and currentDate=curdate()"
    val RUN_ERROR_SQL = "update task_state set endFlag='0',endTime=now() where taskId='vsp_behaviorRec' and currentDate=curdate()"
    var con: Connection = null
    Class.forName(MYSQL_NAME)
    con = DriverManager.getConnection(MYSQL_URL, MYSQL_USER, MYSQL_PASSWORD)
    val stat: Statement = con.createStatement

    def runSuccess(): Unit = {
      stat.execute(RUN_SUCCESS_SQL)
    }

    def runError(): Unit = {
      stat.execute(RUN_ERROR_SQL)
    }

  }

}
