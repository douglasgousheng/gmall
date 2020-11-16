package com.douglas.app

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.JSON
import com.douglas.bean.StartUpLog
import com.douglas.constants.GmallConstants
import com.douglas.handler.DauHandler1
import com.douglas.utils.MyKafkaUtil
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
    val conf: SparkConf = new SparkConf().setAppName("DauApp1").setMaster("local[*]")

    val ssc = new StreamingContext(conf,streaming.Seconds(5))

    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_STARTUP,ssc)

    val sdf = new SimpleDateFormat("yyyy-MM-dd HH")

    val startLogDStream: DStream[StartUpLog] = kafkaDStream.map(record => {
      val value: String = record.value()

      val startUpLog: StartUpLog = JSON.parseObject(value, classOf[StartUpLog])

      val ts: Long = startUpLog.ts

      val dateHourStr: String = sdf.format(new Date(ts))

      val dateHourArr: Array[String] = dateHourStr.split(" ")

      startUpLog.logDate = dateHourArr(0)
      startUpLog.logHour = dateHourArr(1)

      startUpLog
    })

    val filterByRedisDStream: Unit = DauHandler1.filterByRedis(startLogDStream,ssc.sparkContext)

    DauHandler1.filterByMid(startLogDStream)

    DauHandler1.saveMidToRedis(startLogDStream)
  }

}
