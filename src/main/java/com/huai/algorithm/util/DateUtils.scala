package com.huai.algorithm.util

import java.text.SimpleDateFormat
import java.util.Calendar

/**
  * Created by liangyh on 2/18/17.
  */
object DateUtils {

  /**
    *
    * @param preDate
    * @param currDate
    * @return the number of milliseconds
    */
  def subtract(preDate:String, currDate:String):Long ={
    val format = new SimpleDateFormat("yyyyMMddHHmmss")
    if(preDate.length < 4 || currDate.length < 4)return -1
    val date1 = format.parse(preDate)
    val date2 = format.parse(currDate)
    date2.getTime - date1.getTime
  }

  /**
    * 比较第一个时间段是否在第二个时间段里面
    * @param shortTime1
    * @param shortTime2
    * @param longTime1
    * @param longTime2
    * @return
    */
  def isTimeWithin(shortTime1:Long, shortTime2:Long,
                   longTime1:Long, longTime2:Long): Boolean ={
    if(shortTime1 < 0 || shortTime2 < 0
      || longTime1 < 0 || longTime2 < 0){
      throw new IllegalArgumentException(s"$shortTime1, $shortTime2, $longTime1, $longTime2")
    }

    if(shortTime1 >= longTime1 && shortTime2 <= longTime2){
      return true
    }
    false
  }

  def isInTimeScale(time:Long, startOfScale:Long, endOfScale:Long): Boolean ={
    if(time < 0 || startOfScale < 0 || endOfScale < 0){
      throw new IllegalArgumentException(s"$time, $startOfScale, $endOfScale")
    }
    if(time >= startOfScale && time <= endOfScale) true else false
  }

  /**
    * 判断两个时间段是否重合
    * @param shortTime1
    * @param shortTime2
    * @param longTime1
    * @param longTime2
    * @return
    */
  def isTimeCoincide(shortTime1:Long, shortTime2:Long,
                     longTime1:Long, longTime2:Long):Boolean ={
    if(shortTime1 <= 0 || shortTime2 <= 0
      || longTime1 <= 0 || longTime2 <= 0){
      throw new IllegalArgumentException(s"$shortTime1, $shortTime2, $longTime1, $longTime2")
    }

    if((shortTime1 >= longTime1 && shortTime1 <= longTime2) ||
      (shortTime2 <= longTime2 && shortTime1 >= longTime1))  true else false
  }

  def getHourOfDay(dateStr:String): Int ={
    if(dateStr.length < 4)return -1
    val format = new SimpleDateFormat("yyyyMMddHHmmss")
    val date = format.parse(dateStr)
    val cal = Calendar.getInstance()
    cal.setTime(date)
    cal.get(Calendar.HOUR_OF_DAY)
  }

  def main(args: Array[String]): Unit = {
  }
}
