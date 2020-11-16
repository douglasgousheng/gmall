package com.douglas.gmallpublisher.controller;

import com.douglas.gmallpublisher.service.PublisherService;
import com.douglas.gmallpublisher.service.impl.PublisherServiceImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import com.alibaba.fastjson.JSONObject;

import java.io.IOException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * @author douglas
 * @create 2020-11-06 14:12
 */
@RestController
public class PublisherController {

    @Autowired
    private PublisherService publisherService;

    @RequestMapping("realtime-total")
    public String getDauTotal(@RequestParam("date") String date) {
        //1.创建集合用于存放结果数据
        ArrayList<Map> result = new ArrayList<>();
        //2.获取新增日活以及交易额
        Integer dauTotal = publisherService.getDauTotal(date);
        Double orderAmount = publisherService.getOrderAmount(date);

        //3.封装新增日活的Map
        HashMap<String, Object> dauMap = new HashMap<>();
        dauMap.put("id", "dau");
        dauMap.put("name", "新增日活");
        dauMap.put("value", dauTotal);

        //4.封装新增设备的Map
        HashMap<String, Object> newMidMap = new HashMap<>();
        newMidMap.put("id", "new_mid");
        newMidMap.put("name", "新增设备");
        newMidMap.put("value", 233);

        //5.封装新增设备的Map
        HashMap<String, Object> gmvMap = new HashMap<>();
        gmvMap.put("id", "order_amount");
        gmvMap.put("name", "新增交易额");
        gmvMap.put("value", orderAmount);

        //6.将两个Map放入List
        result.add(dauMap);
        result.add(newMidMap);
        result.add(gmvMap);

        return JSONObject.toJSONString(result);

    }

    @RequestMapping("realtime-hours")
    public String getDauTotalHourMap(
            @RequestParam("id") String id,
            @RequestParam("date") String date
    ) {
        HashMap<String, Map> result = new HashMap<>();
        String yesterday = LocalDate.parse(date).plusDays(-1).toString();

        Map todayMap = null;
        Map yesterdayMap = null;

        if ("dau".equals(id)) {
            todayMap = publisherService.getDauHours(date);
            yesterdayMap = publisherService.getDauHours(yesterday);
        } else if ("order_amount".equals(id)) {
            todayMap = publisherService.getOrderAmountHour(date);
            yesterdayMap = publisherService.getOrderAmountHour(yesterday);
        }

        result.put("yesterday", yesterdayMap);
        result.put("today", todayMap);
        return JSONObject.toJSONString(result);

    }

    @RequestMapping("sale_detail")
    public String getSaleDetail(@RequestParam("date") String date,
                                @RequestParam("startpage") int startpage,
                                @RequestParam("size") int size,
                                @RequestParam("keyword") String keyword) throws IOException {

        return  publisherService.getSaleDetail(date, startpage, size, keyword);

    }

}
