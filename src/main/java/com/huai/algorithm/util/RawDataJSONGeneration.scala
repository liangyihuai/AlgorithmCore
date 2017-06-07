package com.huai.algorithm.util


/**
  * Created by liangyh on 2/25/2017.
  */
object RawDataJSONGeneration extends JSONGeneration{
  val phoneIndex = 0
  val timeIndex = 1
  val longitudeIndex = 2
  val latitudeIndex = 3
  var spliterator:Char = '|'
  val input = "/home/liangyh/Documents/GRADUATE-PROJECT/DataSet/广州移动定位结果-胡永森/user6.txt"
  val output = "/home/liangyh/IdeaProjects/AlgorithmCore/mapshow/data/rawData.js"

  def generate():Unit = {
    val lines = read(input)
    val target = parse(lines)
    save(target, output)
  }


  def parse(lines:Iterator[String]):String ={
    val builder = new StringBuilder()
    builder.append("var rawDataSet = [")

    var lineCount = 0
    lines.filter(line => line != null && line != "").foreach(line => {
      lineCount += 1
      val fields = line.split(spliterator)
      builder.append("{\n\"phone\":\""+fields(phoneIndex)+
        "\",\n \"time\":\""+fields(timeIndex)+
        "\", \n\"longitude\":"+fields(longitudeIndex)+
        ", \n\"latitude\":"+fields(latitudeIndex)+"},")
    })

    if(lineCount > 0){
      builder.deleteCharAt(builder.size-1)
    }
    builder.append("];")
    builder.toString()
  }
}

