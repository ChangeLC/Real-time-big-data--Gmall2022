package com.example.gmall2022_publiction.service.impl;

import com.example.gmall2022_publiction.mapper.OrderWideMapper;
import com.example.gmall2022_publiction.service.ClickHouseService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
/**
 * Author: Felix
 * Desc:订单 Service 接口实现
 */
@Service
public class ClickHouseServiceImpl implements ClickHouseService {

    @Resource
    OrderWideMapper orderWideMapper;

    @Override
    public BigDecimal getOrderAmount(String date) {
        return orderWideMapper.selectOrderAmountTotal(date);
    }

    @Override
    public Map<String,BigDecimal> getOrderAmountHour(String date) {
        List<Map> mapList = orderWideMapper.selectOrderAmountHourMap(date);
        Map<String,BigDecimal> orderAmountHourMap=new HashMap();
        for (Map map : mapList) {
            orderAmountHourMap.put(String.format("%02d", map.get("hr")),
                    (BigDecimal)map.get("am"));
        }
        return orderAmountHourMap;
    }
}