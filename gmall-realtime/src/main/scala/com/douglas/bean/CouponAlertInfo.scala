package com.douglas.bean

/**
 * @author douglas
 * @create 2020-11-10 10:06 
 */
case class CouponAlertInfo(mid:String,
                           uids:java.util.HashSet[String],
                           itemIds:java.util.HashSet[String],
                           events:java.util.List[String],
                           ts:Long)
