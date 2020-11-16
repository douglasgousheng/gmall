package com.douglas.app

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.JSON
import com.douglas.bean.StartUpLog
import com.douglas.constants.GmallConstants
import com.douglas.handler.{DauHandler, DauHandler1}
import com.douglas.utils.MyKafkaUtil
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.{SparkConf, streaming}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author douglas
 * @create 2020-11-14 8:27 
 */
object DauApp1 {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf
    val conf: SparkConf = new SparkConf().setAppName("DauApp").setMaster("local[*]")

    //2.创建StreamingContext
    val ssc = new StreamingContext(conf, Seconds(5))

    //3.消费Kafka启动主题数据
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH")
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_STARTUP, ssc)

    //4.将每一行数据转换为样例类对象,并补充时间字段
    val startUpLogDStream: DStream[StartUpLog] = kafkaDStream.map(record => {

      val value: String = record.value()

      val startUpLog: StartUpLog = JSON.parseObject(value, classOf[StartUpLog])

      val ts: Long = startUpLog.ts

      val dateHourStr: String = sdf.format(new Date(ts))

      val dateHourArr: Array[String] = dateHourStr.split(" ")

      startUpLog.logDate = dateHourArr(0)
      startUpLog.logHour = dateHourArr(1)

      startUpLog
    })




    //5.根据Redis进行跨批次去重
    val filterByRedisDStream: DStream[StartUpLog] = DauHandler1.filterByRedis(startUpLogDStream)

    //    startLogDStream.cache()
    //    startLogDStream.count().print()
    //
    //    filterdByRedis.cache()
    //    filterdByRedis.count().print()

    //6.同批次去重(根据Mid)
    DauHandler1.filterByMid(filterByRedisDStream)

    //    filterdByMid.cache()
    //    filterdByMid.count().print()

    //7.将去重之后的数据中的Mid保存到Redis(为了后续批次去重)
    DauHandler1.saveMidToRedis(startUpLogDStream)

    //8.将去重之后的数据明细写入Pheonix

    //打印Value
    //    kafkaDStream.foreachRDD(rdd => {
    //      rdd.foreach(record => {
    //        println(record.value())
    //      })
    //    })

    //开启任务
    ssc.start()
    ssc.awaitTermination()
  }

}
