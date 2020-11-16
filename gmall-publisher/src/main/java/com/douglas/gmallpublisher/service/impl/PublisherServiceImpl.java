package com.douglas.gmallpublisher.service.impl;

import com.alibaba.fastjson.JSON;
import com.douglas.gmallpublisher.bean.Option;
import com.douglas.gmallpublisher.bean.Stat;
import com.douglas.gmallpublisher.mapper.DauMapper;
import com.douglas.gmallpublisher.mapper.OrderMapper;
import com.douglas.gmallpublisher.service.PublisherService;
import io.searchbox.client.JestClient;
import io.searchbox.core.Index;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.MetricAggregation;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author douglas
 * @create 2020-11-06 14:07
 */
@Service
public class PublisherServiceImpl implements PublisherService {
    @Autowired
    private DauMapper dauMapper;

    @Autowired
    private OrderMapper orderMapper;

    @Autowired
    private JestClient jestClient;

    @Override
    public Integer getDauTotal(String date) {
        return dauMapper.selectDauTotal(date);
    }

    @Override
    public Map getDauHours(String date) {
        HashMap dauHourMap = new HashMap();
        List<Map> dauHourList = dauMapper.selectDauTotalHourMap(date);
        for (Map map : dauHourList) {
            dauHourMap.put(map.get("LH"), map.get("CT"));
        }
        return dauHourMap;
    }

    @Override
    public Double getOrderAmount(String date) {
        return orderMapper.selectOrderAmountTotal(date);
    }

    @Override
    public Map getOrderAmountHour(String date) {
        HashMap orderHourMap = new HashMap();
        List<Map> orderHourList = orderMapper.selectOrderAmountHourMap(date);
        for (Map map : orderHourList) {
            orderHourMap.put(map.get("CREATE_HOUR"), map.get("SUM_AMOUNT"));
        }
        return orderHourMap;
    }

    @Override
    public String getSaleDetail(String date, int startpage, int size, String keyword) throws IOException {

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        //过滤 匹配
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.filter(new TermQueryBuilder("dt", date));
        boolQueryBuilder.must(new MatchQueryBuilder("sku_name", keyword).operator(MatchQueryBuilder.Operator.AND));
        searchSourceBuilder.query(boolQueryBuilder);

        //  性别聚合
        TermsBuilder genderAggs = AggregationBuilders.terms("groupby_user_gender").field("user_gender").size(2);
        searchSourceBuilder.aggregation(genderAggs);

        //  年龄聚合
        TermsBuilder ageAggs = AggregationBuilders.terms("groupby_user_age").field("user_age").size(100);
        searchSourceBuilder.aggregation(ageAggs);

        // 行号= （页面-1） * 每页行数
        searchSourceBuilder.from((startpage - 1) * size);
        searchSourceBuilder.size(size);

        System.out.println(searchSourceBuilder.toString());

        Search search = new Search.Builder(searchSourceBuilder.toString())
                .addIndex("gmall2020_sale_detail-query")
                .addType("_doc")
                .build();

        SearchResult searchResult = jestClient.execute(search);

        HashMap<String, Object> result = new HashMap<>();

        Long total = searchResult.getTotal();

        List<SearchResult.Hit<Map, Void>> hits = searchResult.getHits(Map.class);
        ArrayList<Map> details = new ArrayList<>();
        for (SearchResult.Hit<Map, Void> hit : hits) {
            details.add(hit.source);
        }

        MetricAggregation aggregations = searchResult.getAggregations();

        TermsAggregation groupby_user_gender = aggregations.getTermsAggregation("groupby_user_gender");
        long maleCount = 0L;
        for (TermsAggregation.Entry entry : groupby_user_gender.getBuckets()) {
            if (entry.getKey().equals("M")) {
                maleCount = entry.getCount();
            }
        }

        Double maleRatio = Math.round(maleCount * 1000D / total) / 10D;
        Double femaleRatio = Math.round((100D - maleRatio) * 10D) / 10D;

        Option maleOpt = new Option("男", maleRatio);
        Option femaleOpt = new Option("女", femaleRatio);

        ArrayList<Option> genderOptions = new ArrayList<>();
        genderOptions.add(maleOpt);
        genderOptions.add(femaleOpt);
        Stat genderStat = new Stat("用户性别占比", genderOptions);

        TermsAggregation groupby_user_age = aggregations.getTermsAggregation("groupby_user_age");
        Long lower20 = 0L;
        Long upper30 = 0L;
        for (TermsAggregation.Entry entry : groupby_user_age.getBuckets()) {
            if (Integer.parseInt(entry.getKey()) < 20) {
                lower20 += entry.getCount();
            } else if (Integer.parseInt(entry.getKey()) >= 30) {
                upper30 += entry.getCount();
            }
        }
        Double lower20Ratio = Math.round(lower20 * 1000D / total) / 10D;
        Double upper30Ratio = Math.round(upper30 * 1000D / total) / 10D;
        Double upper20to30 = Math.round((100D - lower20Ratio - upper30Ratio) * 10D) / 10D;

        Option lower20Opt = new Option("20岁以下", lower20Ratio);
        Option upper20to30Opt = new Option("20岁到30岁", upper20to30);
        Option upper30Opt = new Option("30岁及30岁以上", upper30Ratio);

        ArrayList<Option> ageOptions = new ArrayList<>();
        ageOptions.add(lower20Opt);
        ageOptions.add(upper20to30Opt);
        ageOptions.add(upper30Opt);

        Stat ageStat = new Stat("用户年龄占比", ageOptions);

        ArrayList<Stat> stats = new ArrayList<>();
        stats.add(ageStat);
        stats.add(genderStat);

        result.put("total", total);
        result.put("stat", stats);
        result.put("detail", details);

        return JSON.toJSONString(result);
    }
}
