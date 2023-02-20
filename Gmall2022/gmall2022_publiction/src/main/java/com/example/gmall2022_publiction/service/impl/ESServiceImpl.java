package com.example.gmall2022_publiction.service.impl;

import com.example.gmall2022_publiction.service.ESService;
import io.searchbox.client.JestClient;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.support.ValueType;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


@Service //将当前对象的创建交给Spring容器进行管理
public class ESServiceImpl implements ESService {
    // 将ES客户端操作对象注入到Service中
    @Autowired
    JestClient jestClient;


    @Override
    public Long getDauTotal(String date) {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        searchSourceBuilder.query(new MatchAllQueryBuilder());

        String query = searchSourceBuilder.toString() ;

        String indexName = "gmall2022_dau_info_"+date+"-query";

        Search search = new Search.Builder(query).addIndex(indexName).addType("_doc").build();

        Long total = 0L;
        try {
            SearchResult result = jestClient.execute(search);
            total = result.getTotal();
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("查询ES失败");
        }

        return total;
    }


    /*
    * GET /gmall2022_dau_info_2022-08-01-query/_search
        {
          "aggs": {
            "groupBy_hr": {
              "terms": {
                "field": "hr",
                "size": 24
              }
            }
          }
        }
     */
    @Override
    public Map<String, Long> getDauHour(String date) {

        Map<String,Long> hourMap = new HashMap<>();

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        TermsAggregationBuilder termsAggregationBuilder = new TermsAggregationBuilder("groupBy_hr", ValueType.LONG).field("hr").size(24);

        searchSourceBuilder.aggregation(termsAggregationBuilder);
        String query = searchSourceBuilder.toString();
        String indexName = "gmall2022_dau_info_"+date+"-query";
        Search search = new Search.Builder(query).addIndex(indexName).build();


        try {
            SearchResult result = jestClient.execute(search);
            TermsAggregation groupBy_hr = result.getAggregations().getTermsAggregation("groupBy_hr");
            if(groupBy_hr != null){
                List<TermsAggregation.Entry> buckets = groupBy_hr.getBuckets();
                for (TermsAggregation.Entry bucket: buckets
                     ) {
                    hourMap.put(bucket.getKey(),bucket.getCount());
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("查询失败");
        }


        return hourMap;
    }
}
