package com.douglas.app

import com.alibaba.fastjson.JSON
import com.douglas.bean.{OrderInfo, SaleDetail}
import com.douglas.constants.GmallConstants
import com.douglas.utils.MyKafkaUtil
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.phoenix.spark._
/**
 * @author douglas
 * @create 2020-11-16 20:33 
 */
object GmvApp {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf
    val conf: SparkConf = new SparkConf().setAppName("GmvApp").setMaster("local[*]")

    //2.创建StreamingContext
    val ssc = new StreamingContext(conf, Seconds(5))

    //3.消费Kafka订单主题数据创建流
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_ORDER_INFO, ssc)


    //4.将每行数据转换为样例类对象,补充时间,数据脱敏(手机号)
    val orderInfoDStream: DStream[OrderInfo] = kafkaDStream.map(record => {
      val orderInfo: OrderInfo = JSON.parseObject(record.value(), classOf[OrderInfo])
      val create_time: String = orderInfo.create_time
      val DateHourArr: Array[String] = create_time.split(" ")
      orderInfo.create_date = DateHourArr(0)
      orderInfo.create_hour = DateHourArr(1).split(":")(0)

      val consignee_tel: String = orderInfo.consignee_tel

      val tuple: (String, String) = consignee_tel.splitAt(4)
      orderInfo.consignee_tel = tuple._1 + "*******"

      orderInfo
    })

    //Seq("ID", "PROVINCE_ID", "CONSIGNEE", "ORDER_COMMENT", "CONSIGNEE_TEL", "ORDER_STATUS", "PAYMENT_WAY", "USER_ID", "IMG_URL", "TOTAL_AMOUNT", "EXPIRE_TIME", "DELIVERY_ADDRESS", "CREATE_TIME", "OPERATE_TIME", "TRACKING_NO", "PARENT_ORDER_ID", "OUT_TRADE_NO", "TRADE_BODY", "CREATE_DATE", "CREATE_HOUR")
    //5.写入Phoenix
    orderInfoDStream.foreachRDD(rdd=>{
      rdd.saveToPhoenix("GMALL2020_ORDER_INFO",
        classOf[OrderInfo].getDeclaredFields.map(_.getName.toUpperCase()),
        HBaseConfiguration.create(),
        Some("hadoop102,hadoop103,hadoop104:2181"))
    })

    //6.开启任务
    ssc.start()
    ssc.awaitTermination()
  }

}
