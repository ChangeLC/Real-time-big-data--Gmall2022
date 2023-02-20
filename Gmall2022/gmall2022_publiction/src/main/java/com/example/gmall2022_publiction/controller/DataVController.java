package com.example.gmall2022_publiction.controller;

import com.example.gmall2022_publiction.service.MySQLService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
/**
 * Author: Felix
 * Desc: 对接 DataV 的 Controller
 */
@RestController
public class DataVController {
    @Autowired
    MySQLService mysqlService;

    @GetMapping("/trademark-sum")
    public Object trademarkSum(@RequestParam("start_date") String startDate,
                               @RequestParam("end_date") String endDate,
                               @RequestParam("topN") int topN){
        List<Map> trademardSum = mysqlService.getTrademardStat(startDate, endDate,topN);

        return trademardSum ;
    }

    @GetMapping("/trademark-sum1")
    public Object trademarkSum1(@RequestParam("start_date") String startDate,
                                @RequestParam("end_date") String endDate,
                                @RequestParam("topN") int topN){

        List<Map> trademarkSumList = mysqlService.getTrademardStat(startDate, endDate,topN);
        //根据 DataV 图形数据要求进行调整， x :品牌 ,y 金额， s 1
        List<Map> datavList=new ArrayList<>();
        for (Map trademardSumMap : trademarkSumList) {
            Map map = new HashMap<>();
            map.put("x",trademardSumMap.get("trademark_name"));
            map.put("y",trademardSumMap.get("amount"));
            map.put("s",1);
            datavList.add(map);
        }
        return datavList; }
}