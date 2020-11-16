package com.douglas.app

import java.sql.{Connection, Date}
import java.text.SimpleDateFormat
import java.util

import com.alibaba.fastjson.JSON
import com.douglas.bean.{OrderDetail, OrderInfo, SaleDetail, UserInfo}
import com.douglas.constants.GmallConstants
import com.douglas.utils.{JdbcUtil, MyEsUtil, MyKafkaUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json4s.DefaultFormats
import redis.clients.jedis.Jedis
import org.json4s.native.Serialization

import collection.JavaConverters._
import scala.collection.mutable.ListBuffer

/**
 * @author douglas
 * @create 2020-11-10 18:47 
 */
//将OrderInfo与OrderDetail数据进行双流JOIN，并根据user_id查询
object SaleDetailApp {
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
          val infoRedisKey: String = s"OrderInfo:$orderId"
          val detailRedisKey: String = s"DetailInfo:$orderId"
          if (infoOpt.isDefined) {
            val orderInfo: OrderInfo = infoOpt.get
            if (detailOpt.isDefined) {
              val orderDetail: OrderDetail = detailOpt.get
              val saleDetail = new SaleDetail(orderInfo, orderDetail)
              details += saleDetail
            }
            val infoStr: String = Serialization.write(orderInfo)
            jedisClient.set(infoRedisKey, infoStr)
            jedisClient.expire(infoRedisKey, 100)

            if (jedisClient.exists(detailRedisKey)) {
              val detailJsonSet: util.Set[String] = jedisClient.smembers(detailRedisKey)
              detailJsonSet.asScala.foreach(detailJson => {
                val detail: OrderDetail = JSON.parseObject(detailJson, classOf[OrderDetail])
                details += new SaleDetail(orderInfo, detail)
              })
            }
          } else {
            val orderDetail: OrderDetail = detailOpt.get
            if (jedisClient.exists(infoRedisKey)) {
              val infoJson: String = jedisClient.get(infoRedisKey)
              val orderInfo: OrderInfo = JSON.parseObject(infoJson, classOf[OrderInfo])
              details += new SaleDetail(orderInfo, orderDetail)
            } else {
              val detailStr: String = Serialization.write(orderDetail)
              jedisClient.set(detailRedisKey, detailStr)
              jedisClient.expire(detailRedisKey, 100)
            }

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
          //查询数据
          val userJson: String = jedisClient.get(userRedisKey)

          val userInfo: UserInfo = JSON.parseObject(userJson, classOf[UserInfo])

          saleDetail.mergeUserInfo(userInfo)


        }else{

          val connection: Connection = JdbcUtil.getConnection

          val userStr: String = JdbcUtil.getUserInfoFromMysql(connection,
            "select * from user_info where id = ?",
            Array(saleDetail.user_id))

          val userInfo: UserInfo = JSON.parseObject(userStr,classOf[UserInfo])

          saleDetail.mergeUserInfo(userInfo)

          connection.close()

        }

        saleDetail

      })

      jedisClient.close()

      details
    })

    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    saleDetailDStream.foreachRDD(rdd=>{
      rdd.foreachPartition(iter=>{
        val indexName = s"${GmallConstants.ES_SALE_DETAIL_INDEX_PRE}-${sdf.format(new Date(System.currentTimeMillis()))}"

        val detailIdToSaleDetail: List[(String, SaleDetail)] = iter.toList.map(saleDetail=>(saleDetail.order_detail_id,saleDetail))

        MyEsUtil.insertBulk(indexName,detailIdToSaleDetail)
      })
    })

//    saleDetailDStream.print(100)

    //    noUserDetailDStream.print(100)


    ssc.start()
    ssc.awaitTermination()
  }

}
