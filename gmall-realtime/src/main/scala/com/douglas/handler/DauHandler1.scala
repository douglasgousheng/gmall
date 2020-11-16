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
  def filterByMid(filterByRedisDStream: DStream[StartUpLog]) = {
    val midDateToLogDStream: DStream[((String, String), StartUpLog)] = filterByRedisDStream.map(log=>((log.mid,log.logDate),log))

    val midDateToLogIterDStream: DStream[((String, String), Iterable[StartUpLog])] = midDateToLogDStream.groupByKey()

    midDateToLogIterDStream.flatMap{case((mid,date),iter)=>
    iter.toList.sortWith(_.ts<_.ts).take(1)}
  }





  private val sdf = new SimpleDateFormat("yyyy-MM-dd")

  /**
   * 根据Redis进行跨批次去重
   *
   * @param startLogDStream 原始数据
   */
  def filterByRedis(startLogDStream: DStream[StartUpLog], sc: SparkContext): DStream[StartUpLog] = {

    //方案一：使用filter单条数据过滤
    //    val value1: DStream[StartUpLog] = startLogDStream.filter(log => {
    //      //a.获取连接
    //      val jedisClient: Jedis = RedisUtil.getJedisClient
    //      //b.判断数据是否存在
    //      val redisKey = s"DAU:${log.logDate}"
    //      val boolean: lang.Boolean = jedisClient.sismember(redisKey, log.mid)
    //      //c.归还连接
    //      jedisClient.close()
    //      //d.返回值
    //      !boolean
    //    })

    //方案二：分区内获取连接
    //    val value2: DStream[StartUpLog] = startLogDStream.transform(rdd => {
    //      rdd.mapPartitions(iter => {
    //        //a.获取连接
    //        val jedisClient: Jedis = RedisUtil.getJedisClient
    //        //b.过滤
    //        val logs: Iterator[StartUpLog] = iter.filter(log => {
    //          val redisKey = s"DAU:${log.logDate}"
    //          !jedisClient.sismember(redisKey, log.mid)
    //        })
    //        //c.归还连接
    //        jedisClient.close()
    //        //d.返回数据
    //        logs
    //      })
    //    })

    //方案三：一个批次获取一次连接,在Driver端获取数据广播至Executor端
    startLogDStream.transform(rdd => {
      val jedisClient: Jedis = RedisUtil.getJedisClient

      val mids: util.Set[String] = jedisClient.smembers(s"DAU${sdf.format(new Date(System.currentTimeMillis()))}")

      jedisClient.close()

      val midsBC: Broadcast[util.Set[String]] = sc.broadcast(mids)

      rdd.filter(log => {
        !midsBC.value.contains(log.mid)
      })

    })

    //    value1
    //    value2

  }


  /**
   * 将去重之后的数据中的Mid保存到Redis(为了后续批次去重)
   *
   * @param startLogDStream 经过2次去重之后的数据集
   */
  def saveMidToRedis(startUpLogDStream: DStream[StartUpLog]) = {
    startUpLogDStream.foreachRDD(rdd => {
      rdd.foreachPartition(iter => {
        val jedisClient: Jedis = RedisUtil.getJedisClient

        iter.foreach(log => {
          val redisKey = s"DAU:${log.logDate}"
          jedisClient.sadd(redisKey, log.mid)
        })

        jedisClient.close()

      })
    })
  }
}
