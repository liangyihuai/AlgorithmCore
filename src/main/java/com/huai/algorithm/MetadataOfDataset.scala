package com.huai.algorithm

/**
  * Created by liangyh on 3/12/17.
  */
class MetadataOfDataset(_phoneIndex:Int, _timeIndex:Int,
                        _longitudeIndex:Int, _latitudeIndex:Int, _separator:String) {

  var phoneIndex: Int = _phoneIndex
  var timeIndex: Int = _timeIndex
  var longitudeIndex: Int = _longitudeIndex
  var latitudeIndex: Int = _latitudeIndex
  var separator: String = _separator

  def this() = this(0, 1, 2, 3, "\\|")




  override def toString = s"MetadataOfDataset($phoneIndex, $timeIndex, $longitudeIndex, $latitudeIndex, $separator)"
}
