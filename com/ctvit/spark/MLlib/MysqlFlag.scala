package com.ctvit.spark.MLlib

/**
 * Created by BaiLu on 2015/8/21.
 */

import java.sql.{DriverManager, Connection}
class MysqlFlag {

  val MYSQL_HOST = "10.3.3.182"
  val MYSQL_PORT = "3306"
  val MYSQL_DB = "ire"
  val MYSQL_DB_USER = "ire"
  val MYSQL_DB_PASSWD = "ZAQ!XSW@CDE#"
  val MYSQL_CONNECT = "jdbc:mysql://" + MYSQL_HOST + ":" + MYSQL_PORT + "/" + MYSQL_DB
  val MYSQL_DRIVER = "com.mysql.jdbc.Driver"


  val init=initMySQL()
  def initMySQL(): Connection = {
    Class.forName(MYSQL_DRIVER)
    DriverManager.getConnection(MYSQL_CONNECT, MYSQL_DB_USER, MYSQL_DB_PASSWD)
  }

  def runSuccess(taskId:String,endTime:String,period:String){
    val MYSQL_SUCCESS = s"update task_state set endTime='$endTime',endFlag='1',period='$period' where taskName='$taskId';"
    init.createStatement().execute(MYSQL_SUCCESS)
  }
  def runFail(taskId:String,errTime:String){
    val MYSQL_FAIL = s"update task_state set errorTime='$errTime',errorFlag='1' where taskName='$taskId';"
    init.createStatement().execute(MYSQL_FAIL)

  }


}
