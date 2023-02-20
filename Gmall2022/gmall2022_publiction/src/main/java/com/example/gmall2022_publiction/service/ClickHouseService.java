package com.example.gmall2022_publiction.service;

import java.math.BigDecimal;
import java.util.Map;

// 订单 Service 接口
public interface ClickHouseService {
    //获取指定日期的总交易额
    public BigDecimal getOrderAmount(String date);

    //获取指定日期的分时交易额
    /*
    * @从mapper中获取分时的交易额格式 List<Map{hr->11,am->1000}> => List<{Map{11->1000}>
    * */
    public Map<String, BigDecimal> getOrderAmountHour(String date);
}
