package com.huai.algorithm.util

import scala.io.Source

/**
  * Created by liangyh on 2/26/17.
  */
object ClusteredDataJSONGeneration extends JSONGeneration{
  def generate():Unit= {
    val input = "/home/liangyh/IdeaProjects/GraduateProject/result.txt"
    val output = "/home/liangyh/IdeaProjects/GraduateProject/src/mapshow/data/clusteredData.js"

    //println
    Source.fromFile(input).getLines().foreach(println)

    val lines = read(input)
    val target = parse(lines)
    save(target, output)
  }

  private val phoneIndex = 0
  private val timeIndex = -1
  private val longitudeIndex = 1
  private val latitudeIndex = 2

  private val spliterator:Char = '\t'

  override def parse(lines: Iterator[String]): String = {
    val builder = new StringBuilder()
    builder.append("var clusteredData = [")

    var lineCount = 0
    lines.foreach(line => {
      lineCount += 1
      val fields = line.split(spliterator)
      builder.append("{\n\"phone\":\""+fields(phoneIndex)+
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
