package com.huai.algorithm

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

import scala.beans.BeanProperty

/**
  * Created by liangyh on 2/21/17.
  */
object DateSetValidation {

  private val logger = LoggerFactory.getLogger(getClass.getName)

  private val inputPath =  "/home/liangyh/Documents/GRADUATE-PROJECT/DataSet/AGG_3G_BSSAP_UTF8-original.txt"

    val phoneIndex = 13
    val timeIndex = 3
    val longitudeIndex = 29
    val latitudeIndex = 30

    val spliterator:Char = '|'

  def main(args:Array[String]):Unit = {
    val conf = new SparkConf().setAppName("GraduateProject").setMaster("local[*]")
    val sc = new SparkContext(conf)

    logger.warn("******************read raw dataset from fs--")
    val rawData = sc.textFile(inputPath, 1)
    logger.warn("-******************-start to do business logic--")

    val validInfo:RDD[Data] = getValidInfo(rawData, this.phoneIndex, this.timeIndex, this.longitudeIndex, this.latitudeIndex)
    println("before count = "+validInfo.count())

    val distinctedRDD = validInfo.distinct()
    /*println("after count = "+distinctedRDD.count())
    val phone = "F8A4B68E41DBB6A0FD806B9B23CCE3EC";

    distinctedRDD.filter(_.phoneNum == phone)
      .sortBy(_.time).repartition(1)
      .saveAsTextFile("/home/liangyh/Documents/GRADUATE-PROJECT/DataSet/"+phone)
    logger.warn("-******************-stop spark context--")*/

    distinctedRDD.repartition(1).saveAsTextFile("/home/liangyh/tmp/gpsData/")

    sc.stop()
  }

  def getValidInfo(data:RDD[String],
                   phoneIndex:Int,
                   timeIndex:Int, longitudeIndex:Int,
                   latitudeIndex:Int):RDD[Data] ={
    data.filter(line =>{!line.isEmpty && line.charAt(line.length-1) != 'N'}).map(line => {
      val fields = line.split(this.spliterator)
      val phoneNum = fields(phoneIndex)
      val time = fields(timeIndex).toLong
      val longitude = fields(longitudeIndex)
      val latitude = fields(latitudeIndex)
      Data(phoneNum, time, longitude, latitude)
    })
  }


  class Data (aPhoneNum:String,
              aTime:Long, aLongitude:String, aLatitude:String) extends Serializable{

    @BeanProperty
    val phoneNum:String =aPhoneNum
    @BeanProperty
    val time:Long = aTime
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
    def apply(phoneNum:String, time:Long, longitude:String, latitude:String): Data = new Data(phoneNum, time, longitude, latitude)
  }
}
