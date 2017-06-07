package com.huai.algorithm

/**
  * Created by liangyh on 3/12/17.
  */
class ParamsOfAlgorithm(_boxLen:Double, _eps:Double, _minPoints:Int, _durationTime:Int,
                       _subtractTimeOfAdjacentStatus:Int, _minPointNumOfACluster:Int,
                        _inputPath:String, _outputPath:String, _runModel:Int) {
  var boxLen: Double = _boxLen
  var eps: Double = _eps
  var minPoints: Int = _minPoints
  var durationTime: Int = _durationTime
  var subtractTimeOfAdjacentStatus: Int = _subtractTimeOfAdjacentStatus
  var minPointNumOfACluster: Int = _minPointNumOfACluster
  var inputPath: String = _inputPath
  var outputPath: String = _outputPath
  var runModel:Int = _runModel;

  def this() = this(0.002, 0.006, 2, 2*3600*1000, 3600*1000, 6, "", "", 0)


  override def toString = s"ParamsOfAlgorithm($boxLen, $eps, $minPoints, $durationTime, $subtractTimeOfAdjacentStatus, $minPointNumOfACluster, $inputPath, $outputPath, $runModel)"
}
