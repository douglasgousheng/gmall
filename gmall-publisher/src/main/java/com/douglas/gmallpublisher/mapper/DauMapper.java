package com.douglas.gmallpublisher.mapper;

import java.util.List;
import java.util.Map;

/**
 * @author douglas
 * @create 2020-11-06 10:13
 */
public interface DauMapper {
    public Integer selectDauTotal (String date);
    public List<Map> selectDauTotalHourMap(String date);
}
