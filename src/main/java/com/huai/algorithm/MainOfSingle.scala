package com.huai.algorithm

import java.io.File

import com.huai.algorithm.util.{DateUtils, FileUtils, ParamsUtils}
import org.apache.commons.math3.ml.clustering.{Cluster, DBSCANClusterer}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
/**
  * /home/liangyh/installed/spark/bin/spark-submit  --class com.huai.graduate.Main /home/liangyh/IdeaProjects/GraduateProject/out/artifacts/GraduateProject_jar/GraduateProject.jar
  *  ./bin/spark-submit --master spark://master:7077 --class com.huai.algorithm.MainOfSingle /home/liangyh/IdeaProjects/GraduateProject/out/artifacts/algorithm_jar/algorithm.jar  phoneIndex#0 timeIndex#2 longitudeIndex#3 latitudeIndex#4 boxLen#0.02 eps#0.06 minPoints#2 durationTime#7200000 subtractTimeOfAdjacentStatus#3600000 minPointNumOfACluster#4 inputPath#file:///home/liangyh/Documents/GRADUATE-PROJECT/DataSet/distincted.txt outputPath#
  *  ./bin/spark-submit --master spark://master:7077 --class com.huai.algorithm.MainOfSingle /home/liangyh/IdeaProjects/AlgorithmCore/out/artifacts/AlgorithmCore_jar/AlgorithmCore.jar  phoneIndex#0 timeIndex#2 longitudeIndex#3 latitudeIndex#4 boxLen#0.02 eps#0.06 minPoints#2 durationTime#7200000 subtractTimeOfAdjacentStatus#3600000 minPointNumOfACluster#4 inputPath#file:///home/liangyh/Documents/GRADUATE-PROJECT/DataSet/distincted.txt outputPath#
  *
  * Created by liangyh on 2/16/17.
  */
object MainOfSingle{

  private val paramsOfAlgorithm = new ParamsOfAlgorithm()
  private val metadataOfDataset = new MetadataOfDataset()

  private val logger = LoggerFactory.getLogger(getClass().getName)
  /*private val outputPath:String = "hdfs://master:9000/output/"*/

  def main(args:Array[String]):Unit = {
    ParamsUtils.parseParamsStr(metadataOfDataset, paramsOfAlgorithm, args)
    if(paramsOfAlgorithm.outputPath.startsWith("hdfs")){
      FileUtils.deleteFiltInHDFS0(paramsOfAlgorithm.outputPath)//delete older output file
    }else{
      val file = new File(paramsOfAlgorithm.outputPath);
      if(file.exists()){
        if(file.isFile)
          file.delete();
        else {
          file.listFiles().foreach(_.delete());
          file.delete();
        }
      }
    }

    val conf = new SparkConf().setAppName("My-Project");
    val sc = new SparkContext(conf)
    try{
      logger.warn("******************read raw dataset from fs--")
      val rawData = sc.textFile(paramsOfAlgorithm.inputPath)
      logger.warn("-******************-start to do business logic--")
      doBusiness(rawData)
      logger.warn("-******************-stop spark context--")
    }catch{
      case e:Exception => {logger.error(e.getStackTrace.toString)}
    }finally {
      sc.stop()
    }
  }

  def doBusiness(data:RDD[String]):Unit ={
    logger.warn("-******************-get valid data--")
    val validInfo:RDD[Data] = getValidInfo(data, metadataOfDataset.phoneIndex,
      metadataOfDataset.timeIndex, metadataOfDataset.longitudeIndex, metadataOfDataset.latitudeIndex)

    logger.warn("get grouped rdd")
    val groupedRDD = validInfo.distinct().map(data => (data.phoneNum, data)).groupByKey()

    logger.warn("calculate clustering rdd")
    val clusteringRDD = groupedRDD.map(item =>{
      //generate grid status
      val statusArray= generateStatus(item._2)
      //filter status
      val filteredStatusArray = filterStatus(statusArray)

      (item._1, filteredStatusArray)
    }).cache()

    clusteringRDD.take(2).foreach(_._2.foreach(println))

    logger.warn("cluster the data")
    clusterWithSingle(clusteringRDD)
  }

  /**
    * DBSCAN算法准备输入数据
    * @param clusteringRDD
    */
  def clusterWithSingle(clusteringRDD: RDD[(String,ArrayBuffer[BaseStatus])]): Unit ={

    clusteringRDD.flatMap(item => {
      val key = item._1
      val statusArr = item._2
      val points = new java.util.ArrayList[AttachmentPoint[BaseStatus]]()
      statusArr.foreach(status => {
        val point = status.box.getCentralPoint()
        val attachPoint = new AttachmentPoint[BaseStatus](point.longitude, point.latitude, status)
        points.add(attachPoint)
      })
      val clusters = doClusterWithSingle(key, points)
      postClusterForSingle(key, clusters)
    }).saveAsTextFile(paramsOfAlgorithm.outputPath)

  }

  /**
    * 单机版dbscan聚类算法
    * @param phone
    * @param points
    * @return
    */
  private def doClusterWithSingle(phone: String, points: java.util.List[AttachmentPoint[BaseStatus]]):
  ArrayBuffer[Cluster[AttachmentPoint[BaseStatus]]] ={
    val dbscan = new DBSCANClusterer[AttachmentPoint[BaseStatus]](paramsOfAlgorithm.eps, paramsOfAlgorithm.minPoints)
    val clusteredResult = dbscan.cluster(points)
    val clusterIter = clusteredResult.iterator()

    //copy clusters into a array
    val clusters = new ArrayBuffer[Cluster[AttachmentPoint[BaseStatus]]]()
    while(clusterIter.hasNext){
      clusters += clusterIter.next()
    }
    clusters
  }

  /**
    * 单机版聚类之后结果的后期处理
    * @param phone
    * @param clusters
    */
  private def postClusterForSingle(phone:String, clusters :ArrayBuffer[Cluster[AttachmentPoint[BaseStatus]]]):ArrayBuffer[String] ={
    //remove the less cluster if the two clusters coinclude
    //避免出现“用户同时停留在两个居住地”的结论
    for(i <- 0 until (clusters.size-1) if clusters(i) != null){
      for(j <- (i+1) until clusters.size if clusters(j) != null){
        if(checkClustersTimeCoinclusion(clusters(i), clusters(j))){
          if(clusters(i).getPoints.size() > clusters(j).getPoints.size()){
            //remove the less cluster
            clusters(j) = null
          }
        }
      }
    }
    //过滤掉点数不够的簇
    val filteredClusters = clusters.filter(cluster => cluster != null &&
      cluster.getPoints.size() >= paramsOfAlgorithm.minPointNumOfACluster)

    val result = new ArrayBuffer[String]()

    filteredClusters.foreach(cluster => {
      //对每个簇进作一步分析, 获取用户的重要位置
      var totalTime = BigDecimal(0)
      var totalXCoordinate = BigDecimal(0)
      var totalYCoordinate = BigDecimal(0)
      val carry = 100000//先增大经纬度，减少因为‘大数’乘以‘小数’所造成的误差。

      val pointIter = cluster.getPoints.iterator()
      while(pointIter.hasNext){
        val point = pointIter.next()
        val subtractTime = DateUtils.subtract(point.attachment.startTime.toString, point.attachment.endTime.toString)/1000;
        totalTime += subtractTime
        totalXCoordinate += point.longitude*carry*subtractTime
        totalYCoordinate += point.latitude*carry*subtractTime
      }

      if(totalTime != 0){
        val finalXCoordinate = (totalXCoordinate./(totalTime*carry)).toDouble
        val finalYCoordinate = (totalYCoordinate./(totalTime*carry)).toDouble

        val validTime = getValidTime(cluster)
        val beginTime = DateUtils.getHourOfDay(validTime._1.toString)
        val endTime = DateUtils.getHourOfDay(validTime._3.toString)
        result += s"$phone\t$finalXCoordinate\t$finalYCoordinate\t$beginTime\t$endTime"
      }
    })

    result
  }
  /**
    * 分析每个重要位置(集群中的所有点)的有效时间范围
    * 该方法寻找集群中所有点的时间段重叠最多的一个时间点，以及重叠的个数。
    * 通俗的解释：每个状态的开始时间和结束时间构成一条横放的线段，寻找一条贯穿这些横线数量最多的竖线。
    * 该竖线所对应的时间点就是重叠最多的时间点。
    * @param cluster
    * @return
    */
  private def getValidTime(cluster: Cluster[AttachmentPoint[BaseStatus]]):(Long, Int, Long, Int)={
    val pointIter1 = cluster.getPoints.iterator()
    val pointIter2 = cluster.getPoints.iterator()
    var maxStartCount:Int = 0
    var maxEndCount:Int = 0
    var startTimeOfMaxCount:Long = 0L
    var endTimeOfMaxCount:Long = 0L
    while(pointIter1.hasNext){
      val point1 = pointIter1.next()
      var startCount = 0
      var endCount = 0
      while(pointIter2.hasNext){
        val point2 = pointIter2.next()
        if(point2 ne point1){
          if(DateUtils.isInTimeScale(point1.attachment.startTime,
            point2.attachment.startTime, point2.attachment.endTime)){
            startCount += 1
          }
          if(DateUtils.isInTimeScale(point1.attachment.endTime,
            point2.attachment.endTime, point2.attachment.endTime)){
            endCount += 1
          }
        }
      }
      if(startCount > maxStartCount) {
        maxStartCount = startCount
        startTimeOfMaxCount = point1.attachment.startTime
      }
      if(endCount > maxEndCount){
        maxEndCount = endCount
        endTimeOfMaxCount = point1.attachment.endTime
      }
    }
    (startTimeOfMaxCount, maxStartCount, endTimeOfMaxCount, maxEndCount)
  }

  /**
    * 检查两个集群（每一个集群都包含多个点，每一个点点都对应相应的一个状态）的时间是否重合
    * @param cluster1
    * @param cluster2
    * @return
    */
  private def checkClustersTimeCoinclusion(cluster1:Cluster[AttachmentPoint[BaseStatus]],
                                           cluster2:Cluster[AttachmentPoint[BaseStatus]]):Boolean = {
    val points1 = cluster1.getPoints
    val points2 = cluster2.getPoints

    val pointIter1:java.util.Iterator[AttachmentPoint[BaseStatus]] = points1.iterator()
    val pointIter2:java.util.Iterator[AttachmentPoint[BaseStatus]] = points2.iterator()
    while(pointIter1.hasNext){
      val point1:AttachmentPoint[BaseStatus] = pointIter1.next()
      while(pointIter2.hasNext){
        val point2:AttachmentPoint[BaseStatus] = pointIter2.next()
        if(DateUtils.isTimeCoincide(point1.attachment.startTime, point1.attachment.endTime,
          point2.attachment.startTime, point2.attachment.endTime)){
          return true
        }
      }
    }
    false
  }



  /**
    * 构建某一个用户的网格状态
    *
    * 需要注意的是，该函数针对的是网格的状态而不是一条数据的状态。
    * 用户的状态变化不应该是某一个网格的状态的改变，而是在多个网格之间。
    * @param data (string,list)-->(phoneNum, list)
    *
    */

  def generateStatus(data:Iterable[Data]): ArrayBuffer[BaseStatus] ={
    implicit val sortDataByDate = new Ordering[Data]{
      override def compare(x: Data, y: Data): Int = x.time compare y.time
    }
    //sort
    val tempData =  ArrayBuffer.empty[Data]
    data.foreach(v => tempData += v)
    val sortedData = tempData.sorted(sortDataByDate)
    //存放结束时间已经确定的状态
    val stateList = ArrayBuffer.empty[BaseStatus]
    val iterator = sortedData.iterator
    val firstData = iterator.next()
    //找出t0时刻连接基站所在网格;
    var gridA = Box.fromLocation(Point(firstData.longitude, firstData.latitude))

    //获取邻居网格
    var neighboursOfA = gridA.getNeighbours()
    //对a及其邻居构建状态网格状态,并将这些状态加入到 gridStatusMap 中;
    var gridStatusMap = buildBaseStatus(null, gridA, neighboursOfA, firstData)

    while(iterator.hasNext){
      val dataB = iterator.next()
      val gridB = Box.fromLocation(Point(dataB.longitude, dataB.latitude))

      val neighboursOfB = gridB.getNeighbours()

      if(gridB.equals(gridA)){//a,b 对应相同网格
        //更新 gridStatusMap 中 a 及其邻居的状态,将结束时间设为 ti ;
        gridStatusMap.values.foreach(_.endTime = dataB.time)
      }else if(haveCommonAdjacent(neighboursOfA, neighboursOfB)){//a,b有共同的邻居
      //更新 gridStatusMap 中 a,b 共同邻居所对应状态的结束时间;
      val commonNeighbours = commonAdjacents(neighboursOfA, neighboursOfB)
        commonNeighbours.foreach(gridStatusMap.get(_).get.endTime = dataB.time)

        if(!isNeighbour(neighboursOfA, gridB)){//此时 a,b 互不为邻居
          //将a及只属于a的邻居所对应的状态从gridStatusMap中移出并加入到stateList中
          moveDataFromCacheToList(gridStatusMap, stateList, (gridKey)=> (!commonNeighbours.contains(gridKey)))
          //为b及只属于 b 的邻居构建状态 , 并加入 gridStatusMap 中 ;
          val adjacentGridKeysNotIntGridA = for(gridKey <- neighboursOfB
                                                if(gridKey != None
                                                  && !commonNeighbours.contains(gridKey.get))) yield gridKey
          gridStatusMap = buildBaseStatus(gridStatusMap, gridB, adjacentGridKeysNotIntGridA, dataB)

        }else{//a，b互为邻居
          //将仅属于 a 的邻居状态从 gridStatusMap "移出"到 stateList 中
          for(key <- neighboursOfA if(key != None && !key.get.equals(gridB) && !commonNeighbours.contains(key.get))){
            stateList += gridStatusMap.get(key.get).get
            gridStatusMap.remove(key.get)
          }
          //为b邻居中仅属于b的网格生成状态,并加入gridStatusMap中;
          val adjacentGridKeysNotIntGridA = for(gridKey <- neighboursOfB
                                                if(gridKey != None && !gridKey.get.equals(gridA) &&
                                                  !commonNeighbours.contains(gridKey.get))) yield gridKey
          gridStatusMap = buildBaseStatus(gridStatusMap, gridB, adjacentGridKeysNotIntGridA, dataB, false)
          //update the end status of gridA
          gridStatusMap.get(gridA).get.endTime = dataB.time
          gridStatusMap.get(gridB).get.endTime = dataB.time
        }
      }else{//此时 a,b 无共同邻居
        //将 a 及其邻居的状态从 gridStatusMap 移到 stateList中
        moveDataFromCacheToList(gridStatusMap, stateList)
        //为 b 及其邻居网格生成状态 , 并加入 gridStatusMap 中.
        gridStatusMap = buildBaseStatus(gridStatusMap, gridB, neighboursOfB, dataB)
      }
      // 用 b 更新上一时刻所在网格
      gridA = gridB
      neighboursOfA = neighboursOfB
    }
    moveDataFromCacheToList(gridStatusMap, stateList)
    stateList
  }

  /**
    * 将某网格和它的邻居网格所缓存的状态转“移出”到list中
    * @param cacheMap
    * @param list
    * @param filter 过滤条件
    */
  private[this] def moveDataFromCacheToList(cacheMap:mutable.HashMap[Box, BaseStatus],
                                            list:ArrayBuffer[BaseStatus],
                                            filter:((Box)=> Boolean)= ((key:Box)=>true)):Unit={
    cacheMap.keysIterator.filter(filter).foreach(list += cacheMap.get(_).get)
    val keys = cacheMap.keysIterator.filter(filter)
    keys.foreach(key =>{cacheMap.remove(key)})
  }

  /**
    * 为某个网格以及它的邻居网格构建网格状态
    * 新返回的网格状态中包含原先的网格状态缓存
    * @param oldGridStatusMap 原先的网格状态缓存
    * @param gridA
    * @param neighbours
    * @param data
    * @return 新的网格状态缓存
    */
  private[this] def buildBaseStatus(oldGridStatusMap:mutable.HashMap[Box,BaseStatus],
                                    gridA:Box,
                                    neighbours:List[Option[Box]],
                                    data:Data,
                                    isBuildForItself:Boolean = true): mutable.HashMap[Box, BaseStatus] ={

    val gridStatusMap = new mutable.HashMap[Box, BaseStatus]()

    if(oldGridStatusMap != null){
      oldGridStatusMap.foreach(entry => gridStatusMap += entry)
    }

    if(isBuildForItself){
      //构建a网格的网络状态
      val gridABaseStatus = new BaseStatus(data)
      gridABaseStatus.box = gridA
      gridStatusMap += (gridA -> gridABaseStatus)
    }

    //构建邻居网格的网络状态
    neighbours.filter(_ != None).foreach(adjacentKey =>{
      val neighbourBaseStatus = new BaseStatus()
      neighbourBaseStatus.box = adjacentKey.get
      neighbourBaseStatus.startTime = data.time
      gridStatusMap += (adjacentKey.get -> neighbourBaseStatus)
    })
    gridStatusMap
  }

  /**
    * 判断两个网格是否有相同的邻居
    * @param adjacentKeysOfGridA
    * @param adjacentKeysOfGridB
    * @return
    */
  private[this] def haveCommonAdjacent(adjacentKeysOfGridA:List[Option[Box]],
                                       adjacentKeysOfGridB:List[Option[Box]]):Boolean ={
    for(adjOfA <- adjacentKeysOfGridA if adjOfA != None){
      for(adjOfB <- adjacentKeysOfGridB if adjOfB != None){
        if(adjOfA.get.equals(adjOfB.get)){
          return true
        }
      }
    }
    false
  }

  /**
    * to look up the neighbour grids that gridA and gridB both have.
    *
    * @param adjacentKeysOfGridA
    * @param adjacentKeysOfGridB
    * @return
    */
  private[this] def commonAdjacents(adjacentKeysOfGridA:List[Option[Box]],
                                    adjacentKeysOfGridB:List[Option[Box]]): mutable.HashSet[Box] ={
    var result = new mutable.HashSet[Box]()
    for(adjOfGridA <- adjacentKeysOfGridA if adjOfGridA != None){
      for(adjOfGridB <- adjacentKeysOfGridB if adjOfGridB != None){
        if(adjOfGridA.get.equals(adjOfGridB.get)){
          result += adjOfGridA.get
        }
      }
    }
    result
  }

  /**
    * 判断两个网格是否为邻居
    * @param adjacentKeysOfGridA
    * @param gridB
    * @return
    */
  private[this] def isNeighbour(adjacentKeysOfGridA:List[Option[Box]], gridB:Box): Boolean ={
    adjacentKeysOfGridA.filter(_ != None).foreach(keyOfA => {
      if(keyOfA.get.equals(gridB))return true
    })
    false
  }

  /**
    * 网格的状态过滤
    * @param statusList
    * @return
    */
  def filterStatus(statusList:ArrayBuffer[BaseStatus]):ArrayBuffer[BaseStatus] ={
    //根据网格进行分组
    val groupedData:Map[Box, ArrayBuffer[BaseStatus]] = statusList.groupBy[Box](_.box)
    //用于存储函数结果
    val result:ArrayBuffer[BaseStatus] = new ArrayBuffer[BaseStatus]()

    groupedData.foreach(item => {
      val tempArray = item._2.filter(_.endTime > 0)
      //若同一组里的相邻两状态间隔小于某一阈值 (30 分钟 )
      //则将前一状态的结束时间设为后一状态的结束时间,并删除原来的后一种状态
      val size = tempArray.size-1
      for(i <- 0 until size){
        val subTime = DateUtils.subtract(tempArray(i).endTime.toString, tempArray(i+1).startTime.toString)
        if(subTime >= 0 && subTime <= paramsOfAlgorithm.subtractTimeOfAdjacentStatus){
          tempArray(i+1).startTime = tempArray(i).startTime
          tempArray(i) = null
        }
      }

      tempArray
        .filter(v => {v != null &&
          v.endTime>0 &&
          DateUtils.subtract(v.startTime.toString, v.endTime.toString) >= paramsOfAlgorithm.durationTime})
        .foreach(v2 => result += v2)
    })
    result
  }

  /**
    * 从原始数据集中抽取有用的数据，过滤无用的数据
    * @param data
    * @param phoneIndex
    * @param timeIndex
    * @param longitudeIndex
    * @param latitudeIndex
    * @return 数据对象
    */
  def getValidInfo(data:RDD[String], phoneIndex:Int, timeIndex:Int, longitudeIndex:Int, latitudeIndex:Int):RDD[Data] ={
    data.filter(line =>(!line.isEmpty && line.charAt(line.length-1) != 'N')).map(line => {
      val fields = line.split(metadataOfDataset.separator)
      val phoneNum = fields(phoneIndex)
      val time = fields(timeIndex).toLong
      val longitude = fields(longitudeIndex).toDouble
      val latitude = fields(latitudeIndex).toDouble
      Data(phoneNum, time, longitude, latitude)
    })
  }

}