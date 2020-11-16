package com.douglas.handler

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.douglas.bean.StartUpLog
import com.douglas.utils.RedisUtil
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

/**
 * @author douglas
 * @create 2020-11-14 8:37 
 */
object DauHandler1 {

  private val sdf = new SimpleDateFormat("yyyy-MM-dd")

  def filterByRedis(startLogDStream: DStream[StartUpLog],sc:SparkContext) = {
    startLogDStream.foreachRDD(rdd=>{
      val jedisClient: Jedis = RedisUtil.getJedisClient

      val mids: util.Set[String] = jedisClient.smembers(s"DAU:${sdf.format(new Date(System.currentTimeMillis()))}")

      jedisClient.close()

      val midsBC: Broadcast[util.Set[String]] = sc.broadcast(mids)

      rdd.filter(log=>{
        !midsBC.value.contains(log.mid)
      })
    })
  }

  def filterByMid(filterByRedis: DStream[StartUpLog]) = {
    val midDateToLogDStream: DStream[((String, String), StartUpLog)] = filterByRedis.map(log=>((log.mid,log.logDate),log))

    val midDateToLogIterDstream: DStream[((String, String), Iterable[StartUpLog])] = midDateToLogDStream.groupByKey()

    midDateToLogIterDstream.flatMap{case((mid,date),iter)=>
        iter.toList.sortWith(_.ts<_.ts).take(1)

    }
  }

  def saveMidToRedis(startLogDStream: DStream[StartUpLog]): Unit = {
    startLogDStream.foreachRDD(rdd=>{
      rdd.foreachPartition(iter=>{
        val jedisClient: Jedis = RedisUtil.getJedisClient

        iter.foreach(log=>{
          val redisKey = s"DAU:${log.logDate}"
          jedisClient.sadd(redisKey,log.mid)
        })

        jedisClient.close()
      })
    })
  }

}
