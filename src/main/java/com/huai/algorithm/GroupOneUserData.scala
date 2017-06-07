package com.huai.algorithm

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory

import scala.beans.BeanProperty
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * Created by liangyh on 3/1/17.
  */
object GroupOneUserData {
  private val logger = LoggerFactory.getLogger(getClass.getName)

  private val inputPath =  DataParams.input

  def main(args:Array[String]):Unit = {
    val conf = new SparkConf().setAppName("GraduateProject").setMaster("local[*]")
    val sc = new SparkContext(conf)

    logger.warn("******************read raw dataset from fs--")
    val rawData = sc.textFile(inputPath, 1)
    logger.warn("-******************-start to do business logic--")

    val validInfo:RDD[Data] = getValidInfo(rawData, DataParams.phoneIndex, DataParams.timeIndex, DataParams.longitudeIndex, DataParams.latitudeIndex)

    validInfo.sortBy(_.phoneNum).saveAsTextFile("/home/liangyh/Documents/GRADUATE-PROJECT/DataSet/广州移动定位结果-胡永森/")

    logger.warn("-******************-stop spark context--")
    sc.stop()
  }

  def getValidInfo(data:RDD[String],
                   phoneIndex:Int,
                   timeIndex:Int, longitudeIndex:Int,
                   latitudeIndex:Int):RDD[Data] ={
    data.filter(line =>{!line.isEmpty && line.charAt(line.length-1) != 'N'}).map(line => {
      val fields = line.split(DataParams.spliterator)
      val phoneNum = fields(phoneIndex)
      val time = fields(timeIndex)
      val longitude = fields(longitudeIndex)
      val latitude = fields(latitudeIndex)
      Data(phoneNum, time, longitude, latitude)
    })
  }


  class Data (aPhoneNum:String,
              aTime:String, aLongitude:String, aLatitude:String) extends Serializable{

    @BeanProperty
    val phoneNum:String =aPhoneNum
    @BeanProperty
    val time:String = aTime
    @BeanProperty
    val longitude = aLongitude
    @BeanProperty
    val latitude = aLatitude

    override def toString: String = phoneNum+"|"+time+"|"+longitude+"|"+latitude


    def canEqual(other: Any): Boolean = other.isInstanceOf[Data]

    override def equals(other: Any): Boolean = other match {
      case that: Data =>
        (that canEqual this) &&
          phoneNum == that.phoneNum &&
          time == that.time &&
          longitude == that.longitude &&
          latitude == that.latitude
      case _ => false
    }

    override def hashCode(): Int = {
      val state = Seq(phoneNum, time, longitude, latitude)
      state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
    }
  }

  object Data{
    def apply(phoneNum:String, time:String, longitude:String, latitude:String): Data = new Data(phoneNum, time, longitude, latitude)
  }
}
