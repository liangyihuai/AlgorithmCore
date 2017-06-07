package com.huai.algorithm

/**
  * Created by liangyh on 2/16/17.
  */
class BaseStatus() extends Serializable {


  def this(data:Data){
    this()
    startTime = data.time
    endTime = 0
  }

  /**
    * 接入起始时间
    */
  var startTime:Long = _

  /**
    * 接入结束时间
    */
  var endTime:Long = _

  var box:Box = _


  override def toString = s"BaseStatus($startTime, $endTime, $box)"


  def canEqual(other: Any): Boolean = other.isInstanceOf[BaseStatus]

  override def equals(other: Any): Boolean = other match {
    case that: BaseStatus =>
      (that canEqual this) &&
        startTime == that.startTime &&
        endTime == that.endTime &&
        box == that.box
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(startTime, endTime, box)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }
}
