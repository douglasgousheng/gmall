package com.douglas.app

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.alibaba.fastjson.JSON
import com.douglas.bean.{CouponAlertInfo, EventLog}
import com.douglas.constants.GmallConstants
import com.douglas.utils.{MyEsUtil, MyKafkaUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}

import scala.util.control.Breaks.{break, breakable}

/**
 * @author douglas
 * @create 2020-11-16 22:44 
 */
object AlterApp1 {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("AlertApp")

    //2.创建StreamingContext
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    //3.消费Kafka TOPIC_EVENT主题数据创建流
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH")
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_EVENT,ssc)

    //4.将每行数据转换为样例类,补充时间字段,并将数据转换为KV结构(mid,log)
    val eventLogDStream: DStream[(String, EventLog)] = kafkaDStream.map(record => {
      val value: String = record.value()

      val eventLog: EventLog = JSON.parseObject(value, classOf[EventLog])

      val dateHourStr: String = sdf.format(new Date(eventLog.ts))

      val dateHourArr: Array[String] = dateHourStr.split(" ")

      eventLog.logDate = dateHourArr(0)
      eventLog.logHour = dateHourArr(1)

      (eventLog.mid, eventLog)
    })

    //5.开窗5min
    val midToLogByWindowDStream: DStream[(String, EventLog)] = eventLogDStream.window(Minutes(5))

    //6.按照mid分组
    val midToLogIterDStream: DStream[(String, Iterable[EventLog])] = midToLogByWindowDStream.groupByKey()

    //7.组内筛选数据
    midToLogIterDStream.map{case(mid,iter)=>
      val uids: util.HashSet[String] = new util.HashSet[String]()
      val itemIds = new util.HashSet[String]()

    }

    //8.生成预警日志

    //9.写入ES

    //10.启动任务
    ssc.start()
    ssc.awaitTermination()
  }

}
