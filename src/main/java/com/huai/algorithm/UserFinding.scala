package com.huai.algorithm

import java.io.{BufferedWriter, FileOutputStream, OutputStreamWriter}

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

/**
  * Created by liangyh on 2/21/17.
  */
object UserFinding {

  private val logger = LoggerFactory.getLogger(getClass.getName)

  private val inputPath = "/home/liangyh/Documents/GRADUATE-PROJECT/DataSet/distincted.txt"

  def main(args:Array[String]):Unit = {
    val conf = new SparkConf().setAppName("GraduateProject");
    val sc = new SparkContext(conf)
    try{
      logger.warn("******************read raw dataset from fs--")
      val rawData = sc.textFile(inputPath)
      val validData = getValidInfo(rawData,0,-1, 2,3,4)
      logger.warn("-******************-start to do business logic--")
      val count = validData.count()
      logger.warn("count = "+count)
    }finally {
      logger.warn("-******************-stop spark context--")
      sc.stop()
    }
  }

  def getValidInfo(data:RDD[String],
                   phoneIndex:Int, baseIDIndex:Int,
                   timeIndex:Int, longitudeIndex:Int,
                   latitudeIndex:Int):RDD[Data] ={
    data.filter(line =>(!line.isEmpty && line.charAt(line.length-1) != 'N')).map(line => {
      logger.warn(line)
      val fields = line.split('|')
      val phoneNum = fields(phoneIndex)
      val time = fields(timeIndex).toLong
      val longitude = fields(longitudeIndex).toDouble
      val latitude = fields(latitudeIndex).toDouble
      Data(phoneNum, time, longitude, latitude)
    })
  }
}
