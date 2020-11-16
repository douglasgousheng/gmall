package com.douglas.app

import com.alibaba.fastjson.JSON
import com.douglas.bean.UserInfo
import com.douglas.constants.GmallConstants
import com.douglas.utils.{MyKafkaUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

/**
 * @author douglas
 * @create 2020-11-10 18:48 
 */
//将用户表新增及变化数据变化缓存之Redis
object UserInfoApp {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf
    val conf: SparkConf = new SparkConf().setAppName("AlertApp").setMaster("local[*]")

    //2.创建StreamingContext
    val ssc = new StreamingContext(conf,Seconds(5))

    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_USER_INFO,ssc)

    val userJsonDStream: DStream[String] = kafkaDStream.map(_.value())

    userJsonDStream.foreachRDD(rdd=>{
      rdd.foreachPartition(iter=>{
        val jedisClient: Jedis = RedisUtil.getJedisClient

        iter.foreach(userJson=>{

          val userInfo: UserInfo = JSON.parseObject(userJson,classOf[UserInfo])
          val reidKey=s"UserInfo:${userInfo.id}"
          jedisClient.set(reidKey,userJson)
        })

        jedisClient.close()
      })
    })

//    kafkaDStream.foreachRDD(rdd=>{
//      rdd.foreach(record=>println(record.value()))
//    })
    ssc.start()
    ssc.awaitTermination()

  }

}
