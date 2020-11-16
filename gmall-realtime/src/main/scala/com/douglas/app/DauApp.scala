package com.douglas.app

import java.util
import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.JSON
import com.douglas.bean.StartUpLog
import com.douglas.constants.GmallConstants
import com.douglas.handler.DauHandler
import com.douglas.utils.MyKafkaUtil
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.{SparkConf, streaming}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis
import org.apache.phoenix.spark._
import org.apache.spark



/**
 * @author douglas
 * @create 2020-11-05 19:13
 */
object DauApp {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("DauApp")

    //2.创建StreamingContext
    val ssc = new StreamingContext(conf,spark.streaming.Seconds(5))

    //3.消费kafka启动主题数据
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_STARTUP,ssc)

//    //4.打印value
//    kafkaDStream.foreachRDD(rdd=>{
//      rdd.foreach(record=>{
//        println(record.value())
//      })
//    })

    //4.将每一行数据转换为样例类对象，并补充时间字段
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH")
    val startLogDStream: DStream[StartUpLog] = kafkaDStream.map(record => {
      //a.获取Value
      val value: String = record.value()
      //b.取出时间戳字段
      val startUpLog: StartUpLog = JSON.parseObject(value, classOf[StartUpLog])

      val ts: Long = startUpLog.ts
      println(ts)
      //c.将时间戳转换为字符串
      val dateHourStr: String = sdf.format(new Date(ts))
      //d.给时间字段重新赋值
      val dateHourArr: Array[String] = dateHourStr.split(" ")
      startUpLog.logDate = dateHourArr(0)
      startUpLog.logHour = dateHourArr(1)

      startUpLog

    })


    //5.根据redis进行跨批次去重
    val filteredByRedis: DStream[StartUpLog] = DauHandler.filterByRedis(startLogDStream,ssc.sparkContext)


    //6.同批次去重（根据Mid）
 val filterByMid: DStream[StartUpLog] = DauHandler.filterByMid(filteredByRedis)

    filterByMid.cache()
    filterByMid.count().print()

    //7.将去重之后的数据中的Mid保存到Redis(为了后续批次去重)
    DauHandler.savaMidToRedis(filterByMid)


    //8.将去重之后的数据明细写入Pheonix
//    filterByMid.foreachRDD{rdd=>
//      rdd.saveToPhoenix("GMALL2020_DAU",Seq("MID", "UID", "APPID", "AREA", "OS", "CH", "TYPE", "VS", "LOGDATE", "LOGHOUR", "TS") ,new Configuration,Some("hadoop102,hadoop103,hadoop104:2181"))
//    }


    //开启任务
    ssc.start()
    ssc.awaitTermination()
  }

}
