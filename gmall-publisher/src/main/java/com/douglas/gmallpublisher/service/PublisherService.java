package com.douglas.gmallpublisher.service;

import java.io.IOException;
import java.util.Map;

/**
 * @author douglas
 * @create 2020-11-06 14:06
 */
public interface PublisherService {
    public Integer getDauTotal(String date);
    public Map getDauHours(String date);
    public Double getOrderAmount(String date);
    public Map getOrderAmountHour(String date);
    public String getSaleDetail(String date,int startpage,int size,String keyword) throws IOException;
}
