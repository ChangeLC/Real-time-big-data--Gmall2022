package com.example.gmall2022_publiction.service;

import java.util.List;
import java.util.Map;
/**
 * Author: Felix
 * Desc: 从 ads 层中获取数据提供的服务接口
 */
public interface MySQLService {
    public List<Map> getTrademardStat(String startDate, String endDate,int topN);
}

