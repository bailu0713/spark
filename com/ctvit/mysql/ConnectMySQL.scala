package com.ctvit.mysql

import java.sql.DriverManager
import java.util

import net.sf.json.JSONObject

/**
 * Created by BaiLu on 2015/10/10.
 */
object ConnectMySQL {

  val MYSQL_HOST = "172.16.168.236"
  val MYSQL_PORT = "3306"
  val MYSQL_DB = "ire"
  val MYSQL_DB_USER = "ire"
  val MYSQL_DB_PASSWD = "ZAQ!XSW@CDE#"
  val MYSQL_CONNECT = "jdbc:mysql://" + MYSQL_HOST + ":" + MYSQL_PORT + "/" + MYSQL_DB
  val MYSQL_DRIVER = "com.mysql.jdbc.Driver"
  val REDIS_IP = "172.16.168.235"
  val REDIS_IP2 = "172.16.168.236"
  val REDIS_PORT = 6379

  val secondsPerMinute = 60
  val minutesPerHour = 60
  val hoursPerDay = 24
  val dayDuration = 7
  val msPerSecond = 1000
  val totalRecNum = 16


  val SQL_CARTOON = "select name_cn,mediaid,ind,p_pic from mediaBestv where mtype='cartoon' order by rand() limit "
  val SQL_TV = "select name_cn,mediaid,ind,p_pic from mediaBestv where mtype='tv' order by rand() limit "
  val SQL_MOVIE = "select name_cn,mediaid,ind,p_pic from mediaBestv where mtype='movie' order by rand() limit "
  val SQL_VAIETY = "select name_cn,mediaid,ind,p_pic from mediaBestv where mtype='variety' order by rand() limit "
  val SQL_SPORT = "select name_cn,mediaid,ind,p_pic from mediaBestv where mtype<>'cartoon' and mtype<>'tv' and (plots like '%运动%' or tags like '%运动%') and tags not like '%爱情%' and tags not like '%纪实%' and tags not like '%恐怖%' and tags not like '%惊悚%' order by rand() limit "
  val SQL_SCIENCE = "select name_cn,mediaid,ind,p_pic from mediaBestv where tags like '%科教%' and mtype='variety' order by rand() limit "
  val SQL_FINANCE = "select name_cn,mediaid,ind,p_pic from mediaBestv where mtype<>'cartoon' and (plots like '%证券%' or adword like '%股市%' or plots like '%股市%' or plots like '%房地产%') order by rand() limit "
  val SQL_ENTAINMENT = "select name_cn,mediaid,ind,p_pic from mediaBestv where mtype='variety' and tags not like '%纪实%' or (tags like '%纪实%' and tags like '%真人秀%' ) order by rand() limit "


  def main(args: Array[String]) {

    rec_vod_list(SQL_MOVIE,10)

  }
  def rec_vod_list(sql: String, number: Int): String = {

    val finalsql = sql + s"$number;"
    val mediaMap = new util.HashMap[String, util.ArrayList[util.HashMap[String, String]]]()
    val mediaList = new util.ArrayList[util.HashMap[String, String]]()
    val mediaInnerMap = new util.HashMap[String, String]()
    val con = initMySQL()
    val result = con.createStatement().executeQuery(finalsql)
    mediaInnerMap.put("name_cn", result.getString(1))
    mediaInnerMap.put("mediaid", result.getString(2))
    mediaInnerMap.put("index", result.getString(3))
    mediaInnerMap.put("p_pic", result.getString(4))
    mediaList.add(mediaInnerMap)
    mediaMap.put("rec_vodlist", mediaList)
    val vodString = JSONObject.fromObject(mediaMap).toString
    vodString
  }
  def initMySQL() = {
    Class.forName(MYSQL_DRIVER)
    DriverManager.getConnection(MYSQL_CONNECT, MYSQL_DB_USER, MYSQL_DB_PASSWD)
  }
  def columnMatch(column: String): String = column match {
    case "电视剧" => SQL_TV
    case "电影" => SQL_MOVIE
    case "少儿" => SQL_CARTOON
    case "综合" => SQL_VAIETY
    case "体育" => SQL_SPORT
    case "科教" => SQL_SCIENCE
    case "财经" => SQL_FINANCE
    case "娱乐" => SQL_ENTAINMENT
  }

}
