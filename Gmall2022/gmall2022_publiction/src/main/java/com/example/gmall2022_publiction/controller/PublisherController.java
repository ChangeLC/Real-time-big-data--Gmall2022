package com.example.gmall2022_publiction.controller;
import com.example.gmall2022_publiction.service.ClickHouseService;
import com.example.gmall2022_publiction.service.ESService;
import org.apache.commons.lang3.time.DateUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.*;

//发布数据接口
@RestController
public class PublisherController {

    @Autowired
    ESService esService;

    @Autowired
    ClickHouseService clickHouseService;
    /*
    * 访问路径：http://publisher:8070/realtime-total?date=2022-08-01
    * 响应数据：{"id":"dau","name":"新增日活","value":1200},
              {"id":"new_mid","name":"新增设备","value":233}
    * */
    @RequestMapping("/realtime-total")
    public Object realtimeTotal(@RequestParam("date") String dt ){
        List<Map<String,Object>> rsList = new ArrayList<Map<String,Object>>();
        Map<String, Object> dauMap = new HashMap<>();
        dauMap.put("id","dau");
        dauMap.put("name","新增日活");
        Long dauTotal = esService.getDauTotal(dt);
        if (dauTotal == null){
            dauMap.put("value",0L);
        }else {
            dauMap.put("value",dauTotal);
        }
        rsList.add(dauMap);



        Map<String, Object> midMap = new HashMap<>();
        midMap.put("id","new_mid");
        midMap.put("name","新增设备");
        midMap.put("value",666);
        rsList.add(midMap);

        //新增交易额
        Map<String,Object> orderAmountMap = new HashMap();
        orderAmountMap.put("id","order_amount");
        orderAmountMap.put("name","新增交易额");
        orderAmountMap.put("value", clickHouseService.getOrderAmount(dt));
        rsList.add(orderAmountMap);

        return rsList;
    }

    /*
    * 访问路径：http://publisher:8070/realtime-hour?id=dau&date=2022-08-01
    * 响应数据： {"yesterday":{"11":383,"12":123,"17":88,"19":200 },
            "today":{"12":38,"13":1233,"17":123,"19":688 }}
    * */
    @RequestMapping("realtime-hour")
    public Object realtimeHour(@RequestParam("id") String id ,@RequestParam("date") String dt) {
        if(id.equals("dau")){
            //封装返回的数据
            Map<String, Map<String, Long>> rsMap = new HashMap<>();

            // 获取今天日活统计
            Map<String, Long> tdMap = esService.getDauHour(dt);
            rsMap.put("today",tdMap);

            // 获取昨天日活统计
            // 根据当前日期获取昨天日期字符串
            String yd = getYd(dt);
            Map<String, Long> ydMap = esService.getDauHour(yd);
            rsMap.put("yesterday",ydMap);

            return  rsMap;
        }else if(id.equals("order_amount")){
            // 获取今天交易额统计
            HashMap<String, Map<String,BigDecimal>> amountMap = new HashMap<>();
            Map<String, BigDecimal> tdMap = clickHouseService.getOrderAmountHour(dt);
            amountMap.put("today",tdMap);

            // 获取昨天的交易额统计
            String yd = getYd(dt);
            Map<String, BigDecimal> ydMap = clickHouseService.getOrderAmountHour(yd);
            amountMap.put("yesterday",ydMap);
            return amountMap;


        }else{
            return null;
        }


    }


    private String getYd(String td){
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        String yd = null;
        try {
            Date tdDate = dateFormat.parse(td);
            Date ydDate = DateUtils.addDays(tdDate, -1);
            yd = dateFormat.format(ydDate);
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("日期格式转变失败");
        }
        return yd;
    }


}
