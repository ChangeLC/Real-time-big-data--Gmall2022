package com.example.gmall2022_publiction.mapper;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

//订单 Mapper 接口
public interface OrderWideMapper {
    //查询当日交易额总数
    public BigDecimal selectOrderAmountTotal(String date);
    //查询当日交易额分时明细
    public List<Map> selectOrderAmountHourMap(String date);
}
