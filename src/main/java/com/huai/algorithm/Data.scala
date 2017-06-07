package com.huai.algorithm

import scala.beans.BeanProperty

/**
  * Created by liangyh on 2/16/17.
  */
class Data (aPhoneNum:String, aTime:Long, aLongitude:Double, aLatitude:Double) extends Serializable{

  @BeanProperty
  val phoneNum:String =aPhoneNum
  @BeanProperty
  val time:Long = aTime
  @BeanProperty
  val longitude:Double = aLongitude
  @BeanProperty
  val latitude:Double = aLatitude

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
  def apply(phoneNum:String, time:Long, longitude:Double, latitude:Double): Data = new Data(phoneNum, time, longitude, latitude)
}
