package com.douglas.utils;
import java.util.Random;
/**
 * @author douglas
 * @create 2020-11-03 18:15
 */
public class RandomNum {
    public static int getRandInt(int fromNum,int toNum){
        return fromNum + new Random().nextInt(toNum-fromNum+1);
    }
}