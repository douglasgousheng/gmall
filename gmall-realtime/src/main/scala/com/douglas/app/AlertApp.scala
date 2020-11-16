package com.douglas.app

/**
 * @author douglas
 * @create 2020-11-10 10:07 
 */
import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.alibaba.fastjson.JSON
import com.douglas.bean.{CouponAlertInfo, EventLog}
import com.douglas.constants.GmallConstants
import com.douglas.utils.{MyEsUtil, MyKafkaUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}

import scala.util.control.Breaks._

object AlertApp {

  def main(args: Array[String]): Unit = {

    //1.创建SparkConf
    val conf: SparkConf = new SparkConf().setAppName("AlertApp").setMaster("local[*]")

    //2.创建StreamingContext
    val ssc = new StreamingContext(conf,Seconds(5))
    //3.消费Kafka TOPIC_EVENT主题数据创建流
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_EVENT,ssc)
    //4.将每行数据转换为样例类,补充时间字段,并将数据转换为KV结构(mid,log)
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH")
    val midToLogDStream: DStream[(String, EventLog)] = kafkaDStream.map(record => {
      val eventLog: EventLog = JSON.parseObject(record.value(), classOf[EventLog])
      val DateHour: String = sdf.format(new Date(eventLog.ts))
      val DateHourArr: Array[String] = DateHour.split(" ")
      eventLog.logDate = DateHourArr(0)
      eventLog.logHour = DateHourArr(1)
      (eventLog.mid, eventLog)
    })

    //5.开窗5min
    val midToLogByWindowDStream: DStream[(String, EventLog)] = midToLogDStream.window(Minutes(5))
    //6.按照mid分组
    val midToLogIterDStream: DStream[(String, Iterable[EventLog])] = midToLogByWindowDStream.groupByKey()

    //7.组内筛选数据
    val boolToAlertInfoDStream: DStream[(Boolean, CouponAlertInfo)] = midToLogIterDStream.map {
      case (mid, iter) =>

        //创建Set用于存放领券的uid
        val uids = new util.HashSet[String]()
        //创建Set用于存放优惠券涉及的商品ID
        val itemIds = new util.HashSet[String]()
        //创建List用于存放反生过的所有行为
        val events = new util.ArrayList[String]()
        //定义标志位,用于记录是否存在浏览商品行为
        var noClick = true

        //遍历iter
        breakable {
          iter.foreach(log => {
            val evid: String = log.evid
            events.add(evid)
            if ("clickItem".equals(evid)) {
              noClick = false
              break()
            } else if ("coupon".equals(evid)) {
              uids.add(log.uid)
              itemIds.add(log.itemid)
            }
          })
        }

        //产生疑似预警日志

        (uids.size() >= 3 && noClick, CouponAlertInfo(mid, uids, itemIds, events, System.currentTimeMillis()))
    }


    val alterInfoDStream: DStream[CouponAlertInfo] = boolToAlertInfoDStream.filter(_._1).map(_._2)
    //8.生成预警日志
//    alterInfoDStream.print()

    //9.写入ES
    alterInfoDStream.foreachRDD(rdd=>{
      rdd.foreachPartition(iter=>{
        val todayStr: String = sdf.format(new Date(System.currentTimeMillis())).split(" ")(0)
        val indexName = s"${GmallConstants.ES_ALERT_INDEX_PRE}-$todayStr"

        val docList: List[(String, CouponAlertInfo)] = iter.toList.map(alterInfo => {
          val minute: Long = alterInfo.ts / 1000 / 60
          (s"${alterInfo.mid}-$minute", alterInfo)
        })
        MyEsUtil.insertBulk(indexName,docList)
      })
    })
    //10.启动任务
    ssc.start()
    ssc.awaitTermination()
  }
}
