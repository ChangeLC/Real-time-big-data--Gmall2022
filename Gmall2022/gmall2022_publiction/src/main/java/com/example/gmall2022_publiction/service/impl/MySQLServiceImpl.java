package com.example.gmall2022_publiction.service.impl;

import com.example.gmall2022_publiction.service.MySQLService;
import com.example.gmall2022_publiction.mapper.TrademarkStatMapper;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.List;
import java.util.Map;
/**
 * Author: Felix
 * Desc: 从 ads 层中获取数据提供的服务的具体实现
 */
@Service
public class MySQLServiceImpl implements MySQLService {
    @Resource
    TrademarkStatMapper trademarkStatMapper;

    @Override
    public List<Map> getTrademardStat(String startDate, String endDate, int topN)
    {
        return trademarkStatMapper.selectTradeSum(startDate,endDate,topN);
    }
}