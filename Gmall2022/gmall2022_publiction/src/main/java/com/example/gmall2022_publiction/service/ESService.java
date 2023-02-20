package com.example.gmall2022_publiction.service;


import java.util.Map;

//操作ES接口
public interface ESService {

//    查询某天日活数据
    public Long getDauTotal(String date);

//    查询某天某时段的日活数据
    public Map<String,Long> getDauHour(String date);
}

