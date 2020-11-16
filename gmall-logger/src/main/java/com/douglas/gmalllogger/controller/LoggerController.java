package com.douglas.gmalllogger.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.douglas.constants.GmallConstants;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.protocol.types.Field;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

/**
 * @author douglas
 * @create 2020-11-03 18:32
 */
//@Controller
@RestController
@Slf4j
public class LoggerController {

    @Autowired
    private KafkaTemplate<String,String> kafkaTemplate;

    @PostMapping("log")
    public String doLog(@RequestParam("logString") String logString){
        JSONObject jsonObject = JSON.parseObject(logString);
        jsonObject.put("ts",System.currentTimeMillis());
        String jsonString = jsonObject.toJSONString();
        log.info(jsonString);

        if ("startup".equals(jsonObject.getString("type"))){
            kafkaTemplate.send(GmallConstants.KAFKA_TOPIC_STARTUP,logString);
        }else {
            kafkaTemplate.send(GmallConstants.KAFKA_TOPIC_EVENT,logString);
        }
        return "success";
    }

    @RequestMapping("test1")
//    @ResponseBody
    public String test01(){
        System.out.println("1111111");
        return "success";
    }

    @RequestMapping("test2")
    public String test02(@RequestParam("name") String name,
                         @RequestParam("age") int age){
        System.out.println(name+" :"+age);
        return "success";
    }

}
