package com.example.gmall2022_logger.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import javax.annotation.Resource;

/**
 * Desc：该Controller用于接收模拟生成的日志
 */

//@Controller：标识为controller组件，将对象的创建交给spring容器管理，并接收处理请求，如果返回String,默认当做页面跳转处理
//@RestController = @Controller+@ResponseBody 会将返回结果转换为json字符串进行响应
@RestController
@Slf4j            // lombok插件提供功能
public class LoggerController {

    // Sprint提供的对kafka
    @Resource      //将kafkaTemplate注入到Contraller中
    KafkaTemplate kafkaTemplate;


    //通过@RequestMapping匹配请求并交给方法处理
    //在模拟数据生成的代码中，数据被封装为json，通过post传递给该Controller处理，
    // @RequestBody 表示从请求体中获取数据
    @RequestMapping("/applog")
    public String applog(@RequestBody String mockLog){

        //将接收的数据落盘
        log.info(mockLog);

        //根据日志的类型 发送到不同的kafka主题中
        //将接收到的数据装换成json对象
        JSONObject jsonObject = JSON.parseObject(mockLog);
        JSONObject startJson = jsonObject.getJSONObject("start");
        if (startJson != null){
            //启动日志
            kafkaTemplate.send("gmall2022_start",mockLog);
        }else {
            //事件日志
            kafkaTemplate.send("gmall2022_event",mockLog);
        }
        return "success";
    }


}
