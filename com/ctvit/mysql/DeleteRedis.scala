package com.ctvit.mysql

import redis.clients.jedis.Jedis

/**
 * Created by BaiLu on 2015/8/11.
 */
object DeleteRedis {
  val REDIS_IP = "192.168.168.42"
  val REDIS_PORT = 6379

  def main(args: Array[String]): Unit = {
    val jedis = initRedis(REDIS_IP, REDIS_PORT)
    val keyset = jedis.keys("*")
    val keyiter = keyset.iterator()
    while (keyiter.hasNext) {
      var j = 0
      val key = keyiter.next()
      if (jedis.llen(key) > 0) {

        jedis.del(key)
        /**?????
          * ?????
          * while循环为什么总是删除不干净
          * */
//        while (j<jedis.llen(key))
//        for(j<- 0 until  jedis.llen(key).toInt) {
//          jedis.rpop(key)
//        }
        jedis.disconnect()
      }
    }

  }

  def initRedis(redisip: String, redisport: Int): Jedis = {
    val jedis = new Jedis(redisip, redisport)
    jedis
  }

}
