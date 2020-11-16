package com.douglas.gmallpublisher.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * @author douglas
 * @create 2020-11-12 13:05
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Stat {
    private String title;
    private List<Option> options;
}
