package com.douglas.app
import com.alibaba.fastjson.JSON
import com.douglas.bean.OrderInfo
import com.douglas.constants.GmallConstants
import com.douglas.utils.MyKafkaUtil
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.phoenix.spark._
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.{SparkConf, streaming}
import org.apache.spark.streaming.{Seconds, StreamingContext}
/**
 * @author douglas
 * @create 2020-11-07 10:54 
 */
object OrderApp {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("OrderApp").setMaster("local[*]")
    val ssc = new StreamingContext(conf,streaming.Seconds(3))
    
    val inputDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_ORDER_INFO,ssc)

    val orderInfoDStream: DStream[OrderInfo] = inputDStream.map(_.value()).map(orderJson => {
      val orderInfo: OrderInfo = JSON.parseObject(orderJson, classOf[OrderInfo])
      val createTimeArr: Array[String] = orderInfo.create_time.split(" ")
      orderInfo.create_date = createTimeArr(0)
      orderInfo.create_hour = createTimeArr(1).split(":")(0)
      val consignee_tel: String = orderInfo.consignee_tel
      val tuple: (String, String) = consignee_tel.splitAt(4)
      orderInfo.consignee_tel = tuple._1 + "*******"
      orderInfo
    })

    orderInfoDStream.foreachRDD(rdd=>{
      println("aaaaaaaaaaaaa")
      rdd.saveToPhoenix(
        "GMALL2020_ORDER_INFO",
        classOf[OrderInfo].getDeclaredFields.map(_.getName.toUpperCase),
        HBaseConfiguration.create(),
        Some("hadoop102,hadoop103,hadoop104:2181")
      )
    })
    
    ssc.start()
    ssc.awaitTermination()
  }

}
