package com.ctvit.recommendation

import java.text.SimpleDateFormat
import java.util.Date

import com.ctvit.MysqlFlag
import scopt.OptionParser

/**
  * Created by BaiLu on 2015/10/12.
  */
object OttContentRec {

   private case class Params(
                              recNumber: Int = 15,
                              taskId: String = null
                              )


   def main(args: Array[String]) {

     val startTime = System.nanoTime()
     val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
     val defaultParams = Params()
     val mysqlFlag = new MysqlFlag
     val parser = new OptionParser[Params]("ContentRecParams") {
       head("set ContentRecParams")
       opt[Int]("recNumber")
         .text(s"the number of reclist default:${defaultParams.recNumber}")
         .action((x, c) => c.copy(recNumber = x))
       opt[String]("taskId")
         .text("the mysql flag key")
         .required()
         .action((x, c) => c.copy(taskId = x))
     }
     try {
       parser.parse(args, defaultParams)
         .map { params =>
         run(params)
         val endTime = df.format(new Date(System.currentTimeMillis()))
         val period = ((System.nanoTime() - startTime) / 1e6).toString.split("\\.")(0)
         mysqlFlag.runSuccess(params.taskId, endTime, period)
       }.getOrElse {
         parser.showUsageAsError
         sys.exit(-1)
       }
     } catch {
       case _: Exception =>
         val errTime = df.format(new Date(System.currentTimeMillis()))
         parser.parse(args, defaultParams).map { params =>
           mysqlFlag.runFail(params.taskId, errTime)
         }
     }

   }

   def run(params:Params) {

   }


 }
