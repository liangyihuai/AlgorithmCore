package com.huai.algorithm

/**
  * Created by liangyh on 2/20/17.
  */
class Point (_longitude:Double, _latitude:Double)extends Serializable{
  val longitude = _longitude
  val latitude = _latitude




  def canEqual(other: Any): Boolean = other.isInstanceOf[Point]

  override def equals(other: Any): Boolean = other match {
    case that: Point =>
      (that canEqual this) &&
        longitude == that.longitude &&
        latitude == that.latitude
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(longitude, latitude)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

  override def toString = s"Point($longitude, $latitude)"
}

object Point{
  def apply(_longitude: Double, _latitude: Double): Point = new Point(_longitude, _latitude)
}
