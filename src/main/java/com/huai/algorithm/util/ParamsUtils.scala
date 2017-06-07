package com.huai.algorithm.util

import java.util

import com.huai.algorithm.{MetadataOfDataset, ParamsOfAlgorithm}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * Created by liangyh on 3/14/17.
  */
object ParamsUtils {

  def generateParamsArray(metadataOfDataset: MetadataOfDataset, paramsOfAlgorithm: ParamsOfAlgorithm):util.List[String] = {
    val result = new util.ArrayList[String]()
    metadataOfDataset.getClass.getDeclaredFields.map(field => {
      field.setAccessible(true)
      val name = field.getName
      if(field.getType == Integer.TYPE){
        result.add(name+"#"+field.getInt(metadataOfDataset))
      }else if(field.getType == java.lang.Double.TYPE){
        result.add(name+"#"+field.getDouble(metadataOfDataset))
      }else{
        result.add(name+"#"+field.get(metadataOfDataset))
      }
    })
    paramsOfAlgorithm.getClass.getDeclaredFields.map(field => {
      field.setAccessible(true)
      val name = field.getName
      if(field.getType == Integer.TYPE){
        result.add(name+"#"+field.getInt(paramsOfAlgorithm))
      }else if(field.getType == java.lang.Double.TYPE){
        result.add(name+"#"+field.getDouble(paramsOfAlgorithm))
      }else{
        result.add(name+"#"+field.get(paramsOfAlgorithm))
      }
    })
    result
  }


  def parseParamsStr(metadataOfDataset: MetadataOfDataset, paramsOfAlgorithm: ParamsOfAlgorithm,
                     params:Array[String]):Unit={
    require(metadataOfDataset != null && paramsOfAlgorithm != null && params != null, "the params is null")

    val metadataFieldNames = new mutable.HashSet[String]()
    val paramsFieldNames = new mutable.HashSet[String]()

    val metadataFields = metadataOfDataset.getClass.getDeclaredFields()
    metadataFields.foreach(field => metadataFieldNames += field.getName)

    val paramsFields = paramsOfAlgorithm.getClass.getDeclaredFields()
    paramsFields.foreach(field => paramsFieldNames += field.getName)

    for(param <- params) {
      if (param.contains('#')) {
        val nameAndValue = param.split('#')
        if(nameAndValue.length >= 2){
          val name = nameAndValue(0)
          val value = nameAndValue(1)
          if (metadataFieldNames.contains(name)) {
            val field = metadataOfDataset.getClass.getDeclaredField(name)
            field.setAccessible(true)
            if(field.getType == Character.TYPE){
              field.set(metadataOfDataset, value.charAt(0))
            }else if(field.getType == Integer.TYPE){
              field.set(metadataOfDataset, value.toInt)
            }else{
              field.set(metadataOfDataset, value)
            }
          } else if (paramsFieldNames.contains(name)) {
            val field = paramsOfAlgorithm.getClass.getDeclaredField(name)
            field.setAccessible(true)

            if(field.getType == Integer.TYPE){
              field.set(paramsOfAlgorithm, value.toInt)
            }else if(field.getType == java.lang.Double.TYPE){
              field.set(paramsOfAlgorithm, value.toDouble)
            }else if(field.getType == Character.TYPE){
              field.set(paramsOfAlgorithm, value.charAt(0))
            } else{
              field.set(paramsOfAlgorithm, value)
            }
          }
        }
      }
    }
  }

  def main(args: Array[String]): Unit = {
   /* val arr = new Array[String](7)
    arr(0) = "phoneIndex:9"
    arr(1) = "timeIndex:8"
    arr(2) = "longitudeIndex:7"
    arr(3) = "latitudeIndex:6"
    arr(4) = "separator:,"
    arr(5) = "boxLen:1"
    arr(6) = "minPoints:2"

    val item = parseParamsStr(arr)
    println(item._1)
    println(item._2)*/

    val metadataOfDataset = new MetadataOfDataset()
    val paramsOfAlgorithm = new ParamsOfAlgorithm()
    val arr = generateParamsArray(metadataOfDataset, paramsOfAlgorithm)
    val iter = arr.iterator()
    while(iter.hasNext){
      println(iter.next())
    }
  }
}
