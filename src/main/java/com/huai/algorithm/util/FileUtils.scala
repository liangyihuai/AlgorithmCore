package com.huai.algorithm.util

import java.io.IOException

/**
  * Created by liangyh on 3/17/17.
  */
object FileUtils {

  def deleteFiltInHDFS0(fullPath:String):Boolean = {
    require(!fullPath.isEmpty && fullPath.contains(':'))
    val secondColonIndex = fullPath.indexOf(':', fullPath.indexOf(':')+1)

    val index = fullPath.indexOf('/',secondColonIndex)
    if(index < 0){
      return deleteFileInHDFS(fullPath, "/")
    }

    val path = fullPath.substring(index)
    val master = fullPath.substring(0, index)
    deleteFileInHDFS(master, path)
  }

  def deleteFileInHDFS(master:String,path:String): Boolean ={
    val output = new org.apache.hadoop.fs.Path(master+path)
    val hdfs = org.apache.hadoop.fs.FileSystem.get(
      new java.net.URI(master), new org.apache.hadoop.conf.Configuration())
    // 删除输出目录
    if (hdfs.exists(output)) {
      try{
        hdfs.delete(output, true)
      }catch{
        case e:IOException => return false
      }
    }
    true
  }

  def main(args: Array[String]): Unit = {
    val fullPath = "hdfs://master:9000/output/"
    deleteFiltInHDFS0(fullPath)

  }
}
