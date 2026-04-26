package index

import entity.Trajectory
import org.locationtech.jts.geom._
import org.locationtech.sfcurve.IndexRange

import java.util
import scala.collection.JavaConverters._

class ElementKNN(
    val xmin: Double,
    val ymin: Double,
    val xmax: Double,
    val ymax: Double,
    val level: Int,
    val g: Int,
    val pre: PrecisionModel,
    val elementCode: Long
) {
  var checked: Boolean = false
  var addedPositionCodes: Int = 0
  var checkedPositionCodes: Int = 0

  private val disOfFourSubElements = new util.ArrayList[Double](4)

  val xLength: Double = xmax - xmin
  val yLength: Double = ymax - ymin

  val positionIndex: Array[Int] = Array(3, 5, 7, 15, 11, 13, 14, 9, 6, 1)
  val positionDisMap: util.HashMap[Long, (Double, Int)] = new util.HashMap[Long, (Double, Int)]()
  val children: util.ArrayList[ElementKNN] = new util.ArrayList[ElementKNN](4)

  def insertion(window: XZStarSFC.QueryWindow): Boolean = {
    window.xmax >= xmin && window.ymax >= ymin && window.xmin <= xmax + xLength && window.ymin <= ymax + yLength
  }

  def insertion(window: XZStarSFC.QueryWindow, x1: Double, y1: Double, x2: Double, y2: Double): Boolean = {
    window.xmax >= x1 && window.ymax >= y1 && window.xmin <= x2 && window.ymin <= y2
  }

  def isContained(window: XZStarSFC.QueryWindow): Boolean = {
    window.xmin <= xmin && window.ymin <= ymin && window.xmax >= xmax + xLength && window.ymax >= ymax + yLength
  }

  def insertCodes(window: XZStarSFC.QueryWindow): util.List[IndexRange] = {
    var pos = 0
    val xeMax = xmax + xLength
    val yeMax = ymax + yLength
    val xCenter = xmax
    val yCenter = ymax

    if (insertion(window, xmin, ymin, xCenter, yCenter)) {
      pos |= 1
    }
    if (insertion(window, xCenter, ymin, xeMax, yCenter)) {
      pos |= 2
    }
    if (insertion(window, xmin, yCenter, xCenter, yeMax)) {
      pos |= 4
    }
    if (insertion(window, xCenter, yCenter, xeMax, yeMax)) {
      pos |= 8
    }

    val results = new util.ArrayList[Long](8)
    var pSize = 9L
    if (level < g) {
      pSize = 8L
    }

    var i = 0L
    while (i <= pSize) {
      if ((positionIndex(i.toInt) & pos) != 0) {
        results.add(i + 1L)
      }
      i += 1L
    }

    results.asScala.map(v => IndexRange(v + elementCode - 10L, v + elementCode - 10L, contained = false)).asJava
  }

  def neededToCheck(traj: Envelope, threshold: Double): Boolean = {
    if (checked) {
      return checked
    }
    val enlElement = new Envelope(xmin, xmax + xLength, ymin, ymax + yLength)
    enlElement.expandBy(threshold)
    checked = enlElement.contains(traj)
    checked
  }

  def neededToCheckXZ(traj: Envelope, threshold: Double): Boolean = {
    if (checked) {
      return checked
    }
    val enlElement = new Envelope(xmin, xmax + xLength, ymin, ymax + yLength)
    enlElement.expandBy(threshold)
    checked = enlElement.contains(traj)
    checked
  }

  def neededToCheckQuad(traj: Envelope, threshold: Double): Boolean = {
    if (checked) {
      return checked
    }
    val enlElement = new Envelope(xmin, xmax, ymin, ymax)
    enlElement.expandBy(threshold)
    checked = enlElement.contains(traj)
    checked
  }

  def split(): Unit = {
    if (children.isEmpty) {
      val xCenter = (xmax + xmin) / 2.0
      val yCenter = (ymax + ymin) / 2.0
      children.add(new ElementKNN(xmin, ymin, xCenter, yCenter, level + 1, g, pre, elementCode + 9L))
      children.add(new ElementKNN(xCenter, ymin, xmax, yCenter, level + 1, g, pre, elementCode + 9L + 1L * IS(level + 1)))
      children.add(new ElementKNN(xmin, yCenter, xCenter, ymax, level + 1, g, pre, elementCode + 9L + 2L * IS(level + 1)))
      children.add(new ElementKNN(xCenter, yCenter, xmax, ymax, level + 1, g, pre, elementCode + 9L + 3L * IS(level + 1)))
    }
  }

  def split1(): Unit = {
    if (children.isEmpty) {
      val xCenter = (xmax + xmin) / 2.0
      val yCenter = (ymax + ymin) / 2.0
      children.add(new ElementKNN(xmin, ymin, xCenter, yCenter, level + 1, g, pre, elementCode + 1L))
      children.add(new ElementKNN(xCenter, ymin, xmax, yCenter, level + 1, g, pre, elementCode + 1L + 1L * (math.pow(4, g - level).toLong - 1L) / 3L))
      children.add(new ElementKNN(xmin, yCenter, xCenter, ymax, level + 1, g, pre, elementCode + 1L + 2L * (math.pow(4, g - level).toLong - 1L) / 3L))
      children.add(new ElementKNN(xCenter, yCenter, xmax, ymax, level + 1, g, pre, elementCode + 1L + 3L * (math.pow(4, g - level).toLong - 1L) / 3L))
    }
  }

  def IS(i: Int): Long = {
    (39L * math.pow(4, g - i).toLong - 9L) / 3L
  }

  def search(root: ElementKNN, x: Double, y: Double, l: Int): ElementKNN = {
    var i = root.level
    var currentElement = root
    while (i < l) {
      val xCenter = (currentElement.xmin + currentElement.xmax) / 2.0
      val yCenter = (currentElement.ymin + currentElement.ymax) / 2.0
      (x < xCenter, y < yCenter) match {
        case (true, true) => currentElement = currentElement.getChildren.get(0)
        case (false, true) => currentElement = currentElement.getChildren.get(1)
        case (true, false) => currentElement = currentElement.getChildren.get(2)
        case (false, false) => currentElement = currentElement.getChildren.get(3)
      }
      i += 1
    }
    currentElement
  }

  def getChildren: util.ArrayList[ElementKNN] = {
    if (children.isEmpty) {
      split()
    }
    children
  }

  def getChildrenQuadTree: util.ArrayList[ElementKNN] = {
    if (children.isEmpty) {
      split1()
    }
    children
  }

  def checkPositionCode(traj: Trajectory, threshold: Double, spoint: Geometry, epoint: Geometry): util.List[IndexRange] = {
    if ((addedPositionCodes == 0x1FF && level < g) || (addedPositionCodes == 0x3FF && level == g)) {
      return new util.ArrayList[IndexRange]()
    }

    val xeMax = this.xmax + xLength
    val yeMax = this.ymax + xLength
    val xCenter = this.xmax
    val yCenter = this.ymax
    val ymaxLocal = yeMax
    val xmaxLocal = xeMax

    var d0 = 0.0
    var d1 = 0.0
    var d2 = 0.0
    var d3 = 0.0
    var outPositions = 0

    if (disOfFourSubElements.isEmpty) {
      val c0 = new Envelope(xmin, xCenter, ymin, yCenter)
      val c1 = new Envelope(xCenter, xmaxLocal, ymin, yCenter)
      val c2 = new Envelope(xmin, xCenter, yCenter, ymaxLocal)
      val c3 = new Envelope(xCenter, xmaxLocal, yCenter, ymaxLocal)
      d0 = dis(c0, traj.getMultiPoint)
      d1 = dis(c1, traj.getMultiPoint)
      d2 = dis(c2, traj.getMultiPoint)
      d3 = dis(c3, traj.getMultiPoint)
      disOfFourSubElements.add(d0)
      disOfFourSubElements.add(d1)
      disOfFourSubElements.add(d2)
      disOfFourSubElements.add(d3)
    } else {
      d0 = disOfFourSubElements.get(0)
      d1 = disOfFourSubElements.get(1)
      d2 = disOfFourSubElements.get(2)
      d3 = disOfFourSubElements.get(3)
    }

    if (d0 > threshold) outPositions |= 1
    if (d1 > threshold) outPositions |= 1 << 1
    if (d2 > threshold) outPositions |= 1 << 2
    if (d3 > threshold) outPositions |= 1 << 3

    val center = new Coordinate(xCenter, yCenter)
    val upperCenter = new Coordinate(xCenter, ymaxLocal)
    val lowerCenter = new Coordinate(xCenter, ymin)
    val centerLeft = new Coordinate(xmin, yCenter)
    val centerRight = new Coordinate(xmaxLocal, yCenter)
    val lowerLeft = new Coordinate(xmin, ymin)
    val upperLeft = new Coordinate(xmin, ymaxLocal)
    val upperRight = new Coordinate(xmaxLocal, ymaxLocal)
    val lowerRight = new Coordinate(xmaxLocal, ymin)

    val results = new util.ArrayList[Long](8)
    var pSize = 9L
    if (level < g) {
      pSize = 8L
    }

    var i = 0L
    while (i <= pSize) {
      val sig = positionIndex(i.toInt)
      if (!((sig & outPositions) > 0) && ((addedPositionCodes & (1 << i.toInt)) == 0)) {
        var shouldCheck = true
        if ((checkedPositionCodes & (1 << i.toInt)) > 0) {
          shouldCheck = positionDisMap.get(i)._1 <= threshold
        }

        if (sig == 15) {
          this.addedPositionCodes |= (1 << i.toInt)
          results.add(i + 1L)
          shouldCheck = false
        }

        if (shouldCheck) {
          val cps = sig match {
            case 1 => Array(lowerLeft, centerLeft, center, lowerCenter, lowerLeft)
            case 3 => Array(lowerLeft, centerLeft, centerRight, lowerRight, lowerLeft)
            case 5 => Array(lowerLeft, upperLeft, upperCenter, lowerCenter, lowerLeft)
            case 6 => Array(centerLeft, upperLeft, upperCenter, center, centerRight, lowerRight, lowerCenter, center, centerLeft)
            case 7 => Array(lowerLeft, upperLeft, upperCenter, center, centerRight, lowerRight, lowerLeft)
            case 9 => Array(lowerLeft, centerLeft, center, upperCenter, upperRight, centerRight, center, lowerCenter, lowerLeft)
            case 11 => Array(lowerLeft, centerLeft, center, upperCenter, upperRight, lowerRight, lowerLeft)
            case 13 => Array(lowerLeft, upperLeft, upperRight, centerRight, center, lowerCenter, lowerLeft)
            case 14 => Array(centerLeft, upperLeft, upperRight, lowerRight, lowerCenter, center, centerLeft)
            case 15 => Array(lowerLeft, upperLeft, upperRight, lowerRight, lowerLeft)
          }

          val previous = if ((checkedPositionCodes & (1 << i.toInt)) > 0) positionDisMap.get(i)._1 else 0.0
          val startIndex = if ((checkedPositionCodes & (1 << i.toInt)) > 0) positionDisMap.get(i)._2 else 0
          val distance = disOfPosAndTraj(cps, threshold, startIndex, traj)
          positionDisMap.put(i, (Math.max(distance._1, previous), distance._3))
          checkedPositionCodes |= (1 << i.toInt)

          if (distance._2) {
            this.addedPositionCodes |= (1 << i.toInt)
            results.add(i + 1L)
          }
        }
      }
      i += 1L
    }

    results.asScala.map(v => IndexRange(v + elementCode - 10L, v + elementCode - 10L, contained = false)).asJava
  }

  private def disOfPosAndTraj(coordinates: Array[Coordinate], threshold: Double, startIndex: Int, traj: Trajectory): (Double, Boolean, Int) = {
    val line = new LinearRing(coordinates, pre, 4326)
    val polygon = new Polygon(line, null, pre, 4326)

    var maxDis = 0.0
    var index = startIndex
    val pivot = traj.getDPFeature.getIndexes
    val upper = pivot.size() - 1
    var i = startIndex
    while (i < upper) {
      index = i
      val disValue = polygon.distance(traj.getGeometryN(pivot.get(i)))
      if (maxDis < disValue) {
        maxDis = disValue
      }
      if (maxDis > threshold) {
        return (maxDis, false, index)
      }
      i += 1
    }
    (maxDis, true, index)
  }

  def dis(env: Envelope, geo: Geometry): Double = {
    val cps = Array(
      new Coordinate(env.getMinX, env.getMinY),
      new Coordinate(env.getMinX, env.getMaxY),
      new Coordinate(env.getMaxX, env.getMaxY),
      new Coordinate(env.getMaxX, env.getMinY),
      new Coordinate(env.getMinX, env.getMinY)
    )
    val line = new LinearRing(cps, pre, 4326)
    val polygon = new Polygon(line, null, pre, 4326)
    polygon.distance(geo)
  }
}
