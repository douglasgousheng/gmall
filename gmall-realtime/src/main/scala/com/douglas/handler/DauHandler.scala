package com.douglas.handler

import java.{lang, util}
import java.text.SimpleDateFormat
import java.util.Date

import com.douglas.bean.StartUpLog
import com.douglas.utils.RedisUtil
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

/**
 * @author douglas
 * @create 2020-11-05 19:40 
 */
object DauHandler {
  def filterByMid(filterdByRedis: DStream[StartUpLog]): DStream[StartUpLog] = {

    //1.转换数据结构 log ==> (mid_logDate,log)
    val midDateToLogDStream: DStream[((String, String), StartUpLog)] = filterdByRedis.map(log => ((log.mid, log.logDate), log))

    //2.按照Key分组
    val midDateToLogIterDStream: DStream[((String, String), Iterable[StartUpLog])] = midDateToLogDStream.groupByKey()


    midDateToLogIterDStream.flatMap { case ((mid, date), iter) =>
      iter.toList.sortWith(_.ts < _.ts).take(1)
    }

  }


  private val sdf = new SimpleDateFormat("yyyy-MM-dd")

  /**
   * 根据redis进行跨批次去重
   * @param startLogDStream
   */
  def filterByRedis(startLogDStream: DStream[StartUpLog],sc:SparkContext) = {
    //方案一：使用filter单条数据去重
//    val value1: DStream[StartUpLog] = startLogDStream.filter(log => {
//      val jedisClient: Jedis = RedisUtil.getJedisClient
//      val redisKey = s"DAU:${log.logDate}"
//      val boolean: lang.Boolean = jedisClient.sismember(redisKey, log.mid)
//      jedisClient.close()
//      !boolean
//    })
    //方案二：分区内获取连接
//    val value2: DStream[StartUpLog] = startLogDStream.transform(rdd => {
//      rdd.mapPartitions(iter => {
//        val jedisClient: Jedis = RedisUtil.getJedisClient
//        val logs: Iterator[StartUpLog] = iter.filter(log => {
//          val redisKey = s"DAU:${log.logDate}"
//          !jedisClient.sismember(redisKey, log.mid)
//        })
//
//        jedisClient.close()
//        logs
//      })
//    })

    //方案三：一个批次获取一次链接，在Driver端获取数据广播至Executor端
    startLogDStream.transform(rdd=>{

      val jedisClient: Jedis = RedisUtil.getJedisClient

      val mids: util.Set[String] = jedisClient.smembers(s"DAU:${sdf.format(new Date(System.currentTimeMillis()) )}")

      jedisClient.close()

      val midsBC: Broadcast[util.Set[String]] = sc.broadcast(mids)

      rdd.filter(log=>{
        !midsBC.value.contains(log.mid)
      })
    })

    startLogDStream.transform(rdd=>{
      val jedisClient: Jedis = RedisUtil.getJedisClient

      val mids: util.Set[String] = jedisClient.smembers(s"DAU:${sdf.format(new Date(System.currentTimeMillis()) )}")

      jedisClient.close()
      val midBC: Broadcast[util.Set[String]] = sc.broadcast(mids)

      rdd.filter(log=>{
        !midBC.value.contains(log.mid)
      })
    })



//    value2
//    value1

  }

  /**
   * 经过两次去重之后的数据
   *
   * @param startLogDStream
   */
  def savaMidToRedis(startLogDStream: DStream[StartUpLog]) = {

    startLogDStream.foreachRDD(rdd=>{
      rdd.foreachPartition(iter=>{
        val jedisClient: Jedis = RedisUtil.getJedisClient

        iter.foreach(log=>{
          val redisKey=s"DAU:${log.logDate}"
          jedisClient.sadd(redisKey,log.mid)
        })

        jedisClient.close()

      })
    })

  }


}
