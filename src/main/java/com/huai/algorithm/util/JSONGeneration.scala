package com.huai.algorithm.util

import java.io._

import scala.io.Source

/**
  * Created by liangyh on 2/26/17.
  */
abstract class JSONGeneration {

  def read(input:String):Iterator[String] ={
    Source.fromFile(new File(input)).getLines()
  }

  def parse(lines:Iterator[String]):String

  def save(data:String, output:String):Unit = {
    var out:Writer = null
    try{
      out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(output)))
      out.write(data)
    }finally {
      out.close()
    }
  }

}


class Data(_phone:String, _time:Long, _longitude:Double, _latitude:Double){
  var phone:String = _phone
  var time:Long = _time
  var longitude = _longitude
  var latitude = _latitude
}
object Data{
  def apply(_phone: String, _time: Long, _longitude: Double, _latitude: Double): Data = new Data(_phone, _time, _longitude, _latitude)
}