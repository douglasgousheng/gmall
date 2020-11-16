package com.douglas.utils

/**
 * @author douglas
 * @create 2020-11-05 18:59 
 */
import java.io.InputStreamReader
import java.util.Properties

object PropertiesUtil {

  def load(propertieName:String): Properties ={
    val prop=new Properties()
    prop.load(new InputStreamReader(Thread.currentThread().getContextClassLoader.getResourceAsStream(propertieName) , "UTF-8"))
    prop
  }


}
