package com.douglas.app

import java.sql.Connection
import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.alibaba.fastjson.JSON
import com.douglas.bean.{OrderDetail, OrderInfo, SaleDetail, UserInfo}
import com.douglas.constants.GmallConstants
import com.douglas.utils.{JdbcUtil, MyEsUtil, MyKafkaUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.json4s.DefaultFormats
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer
import collection.JavaConverters._
import org.json4s.native.Serialization

/**
 * @author douglas
 * @create 2020-11-11 8:56 
 */
object SaleDetailApp1 {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf
    val conf: SparkConf = new SparkConf().setAppName("AlertApp").setMaster("local[*]")

    //2.创建StreamingContext
    val ssc = new StreamingContext(conf, Seconds(5))

    val orderInfoKafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_ORDER_INFO, ssc)
    val orderDetailKafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_ORDER_DETAIL, ssc)

    val orderIdToInfoDStream: DStream[(String, OrderInfo)] = orderInfoKafkaDStream.map(record => {
      val orderInfo: OrderInfo = JSON.parseObject(record.value(), classOf[OrderInfo])
      val create_time: String = orderInfo.create_time
      val dateTimeArr: Array[String] = create_time.split(" ")
      orderInfo.create_date = dateTimeArr(0)
      orderInfo.create_hour = dateTimeArr(1).split(":")(0)

      val consignee_tel: String = orderInfo.consignee_tel
      val tuple: (String, String) = consignee_tel.splitAt(4)
      orderInfo.consignee_tel = tuple._1 + "*******"
      (orderInfo.id, orderInfo)
    })

    val orderIdToDetailDStream: DStream[(String, OrderDetail)] = orderDetailKafkaDStream.map(record => {
      val detail: OrderDetail = JSON.parseObject(record.value(), classOf[OrderDetail])
      (detail.order_id, detail)
    })

    val fulljoinDStream: DStream[(String, (Option[OrderInfo], Option[OrderDetail]))] = orderIdToInfoDStream.fullOuterJoin(orderIdToDetailDStream)

    val noUserDetailDStream: DStream[SaleDetail] = fulljoinDStream.mapPartitions(iter => {

      val jedisClient: Jedis = RedisUtil.getJedisClient

      val details = new ListBuffer[SaleDetail]
      implicit val formats: DefaultFormats.type = org.json4s.DefaultFormats

      iter.foreach {
        case ((orderId, (infoOpt, detailOpt))) =>
          val orderRedisKey = s"OrderInfo:$orderId"
          val detailRedisKey = s"DetailInfo:$orderId"
          if (infoOpt.isDefined) {
            val orderValue: OrderInfo = infoOpt.get
            if (detailOpt.isDefined) {
              val detailValue: OrderDetail = detailOpt.get
              val detail = new SaleDetail(orderValue, detailValue)
              details += detail
            }
            val orderStr: String = Serialization.write(orderValue)
            jedisClient.set(orderRedisKey, orderStr)
            jedisClient.expire(orderRedisKey, 100)

            if (jedisClient.exists(detailRedisKey)) {
              val detailJsonSet: util.Set[String] = jedisClient.smembers(detailRedisKey)
              detailJsonSet.asScala.foreach(detailJson => {
                val detail: OrderDetail = JSON.parseObject(detailJson, classOf[OrderDetail])
                details += new SaleDetail(orderValue, detail)
              }
              )
            }

          } else {
            val detailValue: OrderDetail = detailOpt.get
            if (jedisClient.exists(orderRedisKey)) {
              val orderJson: String = jedisClient.get(orderRedisKey)
              val orderInfo: OrderInfo = JSON.parseObject(orderJson, classOf[OrderInfo])
              details += new SaleDetail(orderInfo, detailValue)
            }
            val detailStr: String = Serialization.write(detailValue)
            jedisClient.set(detailRedisKey, detailStr)
            jedisClient.expire(detailRedisKey, 100)
          }

      }

      jedisClient.close()

      details.iterator
    })

    val saleDetailDStream: DStream[SaleDetail] = noUserDetailDStream.mapPartitions(iter => {
      val jedisClient: Jedis = RedisUtil.getJedisClient

      val details: Iterator[SaleDetail] = iter.map(saleDetail => {
        val userRedisKey = s"UserInfo:${saleDetail.user_id}"
        if (jedisClient.exists(userRedisKey)) {
          val userJson: String = jedisClient.get(userRedisKey)
          val userInfo: UserInfo = JSON.parseObject(userJson, classOf[UserInfo])
          saleDetail.mergeUserInfo(userInfo)

        } else {
          val connection: Connection = JdbcUtil.getConnection

          val userStr: String = JdbcUtil.getUserInfoFromMysql(
            connection,
            "select * from user_info where id=?",
            Array(saleDetail.user_id))
          val userInfo: UserInfo = JSON.parseObject(userStr, classOf[UserInfo])

          saleDetail.mergeUserInfo(userInfo)

          val UserStr: String = Serialization.write(userInfo)
          jedisClient.set(userRedisKey,UserStr)

          connection.close()

        }

        saleDetail
      })


      jedisClient.close()

      details
    })

    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    saleDetailDStream.foreachRDD(rdd => {
      rdd.foreachPartition(iter => {
        val indexName = s"${GmallConstants.ES_SALE_DETAIL_INDEX_PRE}-${sdf.format(new Date(System.currentTimeMillis()))}"
        val detailIdToSaleDetail: List[(String, SaleDetail)] = iter.toList.map(saleDetail => (saleDetail.order_detail_id,saleDetail))

        MyEsUtil.insertBulk(indexName,detailIdToSaleDetail)
      })
    })


    ssc.start()
    ssc.awaitTermination()

  }

}
