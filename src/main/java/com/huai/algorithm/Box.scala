package com.huai.algorithm

/**
  * Created by liangyh on 2/20/17.
  */
class Box private(_x:Int, _y:Int) extends Serializable{

  var x:Int = _x

  var y:Int = _y

  def getNeighbours():List[Option[Box]] = {
    val east = getEasternNeighbour()
    val west = getWesternNeighbour()
    val north = getNorthernNeighbour()
    val south = getSouthernNeighbour()

    var result = List[Option[Box]]()
    if(east != None){
      val box = east.get
      result = box.getSouthernNeighbour() :: box.getNorthernNeighbour() :: east:: result
    }
    if(west != None){
      val box = west.get
      result = box.getNorthernNeighbour() :: box.getSouthernNeighbour() :: west::result
    }
    north::south::result
  }

  def getEasternNeighbour():Option[Box] ={
    if((this.x + 1)*Box.BOX_LENGTH > Box.REGION_MAX_RIGHT){
      return None
    }
    val newBox = new Box(this.x+1, this.y)
    Some(newBox)
  }

  def getWesternNeighbour():Option[Box] = {
    if(this.x == 0)return None
    val newBox = new Box(this.x-1, this.y)
    Some(newBox)
  }

  def getNorthernNeighbour():Option[Box] = {
    if((this.y+1)*Box.BOX_WIDTH > Box.REGION_MAX_UP){
      return None
    }
    val newBox = new Box(this.x, this.y+1)
    Some(newBox)
  }

  def getSouthernNeighbour():Option[Box] = {
    if(this.y == 0)return None

    val newBox = new Box(this.x, this.y-1)
    Some(newBox)
  }

  /**
    *
    * @return (x,y),x表示经度的，y表示纬度的
    */
  def boxCoordinate(): (Int, Int) ={
    if(x < 0 || y < 0)throw new IllegalArgumentException
    (x, y)
  }

  def getCentralPoint(): Point ={
    val x2 = x+1
    val y2 = y+1
    val xx = (x*Box.BOX_LENGTH+x2*Box.BOX_LENGTH)/(2*Box.CARRY)+Box.REGION_MIN_LONGITUDE
    val yy = (y*Box.BOX_WIDTH+y2*Box.BOX_WIDTH)/(2*Box.CARRY)+Box.REGION_MIN_LATITUDE
    new Point(xx,yy)
  }

  def getPoints():Array[Point] ={
    val x1 = (this.x*Box.BOX_LENGTH+Box.REGION_MIN_LONGITUDE*Box.CARRY)/Box.CARRY
    val y1 = (this.y*Box.BOX_WIDTH+Box.REGION_MIN_LATITUDE*Box.CARRY)/Box.CARRY
    val x2 = ((this.x+1)*Box.BOX_LENGTH+Box.REGION_MIN_LONGITUDE*Box.CARRY)/Box.CARRY
    val y2 = ((this.y+1)*Box.BOX_WIDTH+Box.REGION_MIN_LATITUDE*Box.CARRY)/Box.CARRY

    Array(Point(x1,y1), Point(x1,y2), Point(x2,y1), Point(x2,y2))
  }



  def canEqual(other: Any): Boolean = other.isInstanceOf[Box]

  override def equals(other: Any): Boolean = other match {
    case that: Box =>
      (that canEqual this) &&
        x == that.x &&
        y == that.y
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(x, y)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

  override def toString = s"Box($x, $y)"
}


object Box{
  /**
    * 在计算之前先让经纬度值由浮点数变成整数
    */
  val CARRY = 1000000

  /**
    * 区域的最大经度
    */
  private val REGION_MAX_LONGITUDE = 160

  /**
    * 区域的最经经度
    */
  private val REGION_MAX_LATITUDE = 60

  /**
    * 区域的最小纬度
    */
  private val REGION_MIN_LONGITUDE = 10

  /**
    * 区域的最小纬度
    */
  private val REGION_MIN_LATITUDE = 10

  val REGION_MAX_RIGHT = REGION_MAX_LONGITUDE*CARRY - REGION_MIN_LONGITUDE*CARRY
  val REGION_MAX_UP = REGION_MAX_LATITUDE*CARRY - REGION_MIN_LATITUDE*CARRY

  /**
    * box的长，也是长的单位，单位为米
    * 经度相同的情况下，纬度相差0.005度，大约等于500米的距离
    */
  val BOX_LENGTH = 0.002*CARRY

  /**
    * 宽
    */
  val BOX_WIDTH = BOX_LENGTH


  def  fromLocation(point:Point):Box={
    if(point == null) throw new IllegalArgumentException()

    if(point.longitude > REGION_MAX_LONGITUDE || point.latitude > REGION_MAX_LATITUDE||
      point.longitude < REGION_MIN_LONGITUDE || point.latitude < REGION_MIN_LATITUDE){
      throw new IllegalArgumentException("the longitude or the latitude is beyond the range"+point)
    }

    val x = ((point.longitude*CARRY-REGION_MIN_LONGITUDE*CARRY)/BOX_LENGTH).toInt
    val y = ((point.latitude*CARRY-REGION_MIN_LATITUDE*CARRY)/BOX_WIDTH).toInt
    new Box(x, y)
  }

  def fromCoordinate(x:Int, y:Int):Box = {
    if(x < 0 || y < 0)throw new IllegalArgumentException

    new Box(x, y)
  }

  def apply(_x: Int, _y: Int): Box = new Box(_x, _y)
}
