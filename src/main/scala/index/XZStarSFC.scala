package index

import index.XZStarSFC.QueryWindow
import org.locationtech.jts.geom._
import org.locationtech.sfcurve.IndexRange

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

/**
 * XZ_STAR：基于论文里的 XZStarSFC（这里仅集成 SRQ 矩形 ranges + index 编码）。
 *
 * 按你的要求：
 * - XZ_STAR 不依赖其它方法的 tspEncoding
 * - XZ_STAR 固定使用 2*2 的 shape 语义
 * - KNN / 相似度候选生成先不接入
 */
class XZStarSFC(
    g: Short,
    xBounds: (Double, Double),
    yBounds: (Double, Double),
    beta: Int
) extends Serializable {
  private val xLo = xBounds._1
  private val xHi = xBounds._2
  private val yLo = yBounds._1
  private val yHi = yBounds._2

  private val xSize = xHi - xLo
  private val ySize = yHi - yLo

  def index(geometry: Geometry, lenient: Boolean = false): Long = {
    index2(geometry, lenient)._1
  }

  def index2(geometry: Geometry, lenient: Boolean = false): (Long, Long, Int) = {
    val mbr = geometry.getEnvelopeInternal
    val (nxmin, nymin, nxmax, nymax) = normalize(mbr.getMinX, mbr.getMinY, mbr.getMaxX, mbr.getMaxY, lenient)
    val maxDim = math.max(nxmax - nxmin, nymax - nymin)

    val l1 = math.floor(math.log(maxDim) / XZSFC.LogPointFive).toInt

    // the length will either be (l1) or (l1 + 1)
    val length = if (l1 >= g) {
      g.toInt
    } else {
      val w2 = math.pow(0.5, l1 + 1) // width of an element at resolution l2 (l1 + 1)

      // predicate for checking how many axis the polygon intersects
      def predicate(min: Double, max: Double): Boolean = max <= (math.floor(min / w2) * w2) + (2 * w2)

      if (predicate(nxmin, nxmax) && predicate(nymin, nymax)) l1 + 1 else l1
    }

    val w = math.pow(0.5, length)
    val x = math.floor(nxmin / w) * w
    val y = math.floor(nymin / w) * w

    val pc = signature(
      x * xSize + xLo,
      y * ySize + yLo,
      (x + 2 * w) * xSize + xLo,
      (y + 2 * w) * ySize + yLo,
      geometry
    )

    (sequenceCode(nxmin, nymin, length, pc), pc, length)
  }

  /**
   * XZStarSFC 里的扩展元素（用于 signature 的重叠检查/分裂）。
   */
  case class Element2(xmin: Double, ymin: Double, xmax: Double, ymax: Double, level: Int, code: Long) {
    def overlaps(traj: Geometry): Boolean = {
      val cps = Array(
        new Coordinate(xmin, ymin),
        new Coordinate(xmin, ymax),
        new Coordinate(xmax, ymax),
        new Coordinate(xmax, ymin),
        new Coordinate(xmin, ymin)
      )
      val line = new LinearRing(cps, pre, 4326)
      val polygon = new Polygon(line, null, pre, 4326)
      var i = 0
      while (i < traj.getNumGeometries) {
        if (polygon.intersects(traj.getGeometryN(i))) return true
        i += 1
      }
      false
    }

    def children: Seq[Element2] = {
      val xCenter = (xmin + xmax) / 2.0
      val yCenter = (ymin + ymax) / 2.0
      val c0 = copy(xmax = xCenter, ymax = yCenter, level = level + 1, code = code)
      val c1 = copy(xmin = xCenter, ymax = yCenter, level = level + 1, code = code + 1L * math.pow(4, beta - level).toLong)
      val c2 = copy(xmax = xCenter, ymin = yCenter, level = level + 1, code = code + 2L * math.pow(4, beta - level).toLong)
      val c3 = copy(xmin = xCenter, ymin = yCenter, level = level + 1, code = code + 3L * math.pow(4, beta - level).toLong)
      Seq(c0, c1, c2, c3)
    }
  }

  // 对应原实现里的映射表：sig(0..15) -> pc
  private val psMaximum: Array[Int] = Array(0, 10, 0, 1, 0, 2, 9, 3, 0, 8, 0, 5, 0, 6, 7, 4)

  private val pre: PrecisionModel = new PrecisionModel()

  def signature(x1: Double, y1: Double, x2: Double, y2: Double, traj: Geometry): Long = {
    val remaining = new java.util.ArrayDeque[Element2](math.pow(4, beta).toInt)
    val levelOneElements = Element2(x1, y1, x2, y2, 1, 0L).children
    val levelTerminator = Element2(-1.0, -1.0, -1.0, -1.0, 0, 0L)

    levelOneElements.foreach(remaining.add)
    remaining.add(levelTerminator)

    var level = 1
    var sig = 0 // 0..15

    while (!remaining.isEmpty) {
      val next = remaining.poll()
      if (next.eq(levelTerminator)) {
        if (!remaining.isEmpty && level < beta) {
          level = level + 1
          remaining.add(levelTerminator)
        }
      } else {
        if (next.overlaps(traj)) {
          if (level < beta) {
            next.children.foreach(remaining.add)
          } else {
            sig |= (1 << next.code.toInt)
          }
        }
      }
    }

    psMaximum(sig).toLong
  }

  def sequenceCode(x: Double, y: Double, length: Int, posCode: Long): Long = {
    var xmin = 0.0
    var ymin = 0.0
    var xmax = 1.0
    var ymax = 1.0

    var cs = 0L

    def IS(i: Int): Long = {
      (39L * math.pow(4, g - i).toLong - 9L) / 3L
    }

    var i = 1
    while (i <= length) {
      val xCenter = (xmin + xmax) / 2.0
      val yCenter = (ymin + ymax) / 2.0
      (x < xCenter, y < yCenter) match {
        case (true, true) =>
          cs += 9L
          xmax = xCenter
          ymax = yCenter
        case (false, true) =>
          cs += 9L + 1L * IS(i)
          xmin = xCenter
          ymax = yCenter
        case (true, false) =>
          cs += 9L + 2L * IS(i)
          xmax = xCenter
          ymin = yCenter
        case (false, false) =>
          cs += 9L + 3L * IS(i)
          xmin = xCenter
          ymin = yCenter
      }
      i += 1
    }

    cs - 10L + posCode
  }

  /**
   * Normalize user space values to [0,1]
   */
  def normalize(
      xmin: Double,
      ymin: Double,
      xmax: Double,
      ymax: Double,
      lenient: Boolean
  ): (Double, Double, Double, Double) = {
    require(xmin <= xmax && ymin <= ymax, s"Bounds must be ordered: [$xmin $xmax] [$ymin $ymax]")

    try {
      require(xmin >= xLo && xmax <= xHi && ymin >= yLo && ymax <= yHi,
        s"Values out of bounds ([$xLo $xHi] [$yLo $yHi]): [$xmin $xmax] [$ymin $ymax]")

      val nxmin = (xmin - xLo) / xSize
      val nymin = (ymin - yLo) / ySize
      val nxmax = (xmax - xLo) / xSize
      val nymax = (ymax - yLo) / ySize
      (nxmin, nymin, nxmax, nymax)
    } catch {
      case _: IllegalArgumentException if lenient =>
        val bxmin = clamp(xmin, xLo, xHi)
        val bymin = clamp(ymin, yLo, yHi)
        val bxmax = clamp(xmax, xLo, xHi)
        val bymax = clamp(ymax, yLo, yHi)

        val nxmin = (bxmin - xLo) / xSize
        val nymin = (bymin - yLo) / ySize
        val nxmax = (bxmax - xLo) / xSize
        val nymax = (bymax - yLo) / ySize
        (nxmin, nymin, nxmax, nymax)
    }
  }

  private def clamp(value: Double, min: Double, max: Double): Double = {
    if (value < min) min
    else if (value > max) max
    else value
  }

  /**
   * SRQ: 根据矩形窗口生成候选 IndexRange（contained=true/false）。
   *
   * 参数顺序：lng1, lat1, lng2, lat2（与其它 LocS/XZLoc/LETI 保持一致）
   */
  def ranges(lng1: Double, lat1: Double, lng2: Double, lat2: Double): java.util.List[IndexRange] = {
    val queryWindow = QueryWindow(lng1, lat1, lng2, lat2)
    val ranges = new java.util.ArrayList[IndexRange](100)
    val remaining = new java.util.ArrayDeque[ElementKNN](200)

    val levelStop = new ElementKNN(-1, -1, -1, -1, -1, -1, pre, 0L)
    val root = new ElementKNN(xLo, yLo, xHi, yHi, 0, g.toInt, pre, 0L)
    root.split()
    root.children.asScala.foreach(remaining.add)
    remaining.add(levelStop)

    var level: Short = 1

    // XZ_STAR 本身的“准确 VC”：包含（contain）+ 相交（intersect）的 ElementKNN 计数
    var containedQuadCount: Long = 0L
    var intersectQuadCount: Long = 0L

    while (!remaining.isEmpty) {
      val next = remaining.poll()
      if (next.eq(levelStop)) {
        if (!remaining.isEmpty && level < g) {
          level = (level + 1).toShort
          remaining.add(levelStop)
        }
      } else {
        // checkValue
        if (next.isContained(queryWindow)) {
          containedQuadCount += 1L
          val min = next.elementCode + 1L
          val max = next.elementCode + next.IS(level.toInt) - 1L
          ranges.add(IndexRange(min, max, contained = true))
        } else if (next.insertion(queryWindow)) {
          intersectQuadCount += 1L
          ranges.addAll(next.insertCodes(queryWindow))
          if (level < g) {
            next.split()
            next.children.asScala.foreach(remaining.add)
          }
        }
      }
    }

    // 合并多个非重叠连续区间
    if (ranges.size() > 0) {
      ranges.sort(IndexRange.IndexRangeIsOrdered)
      var current = ranges.get(0)
      val result = ArrayBuffer.empty[IndexRange]

      var i = 1
      while (i < ranges.size()) {
        val range = ranges.get(i)
        if (range.lower <= current.upper + 1) {
          current = IndexRange(
            current.lower,
            math.max(current.upper, range.upper),
            current.contained && range.contained
          )
        } else {
          result.append(current)
          current = range
        }
        i += 1
      }
      result.append(current)
      XZStarSFC.setLastRangeStats(XZStarSFC.RangeStats(containedQuadCount, intersectQuadCount, 0L))
      RangeStatsBridge.setLast(
        RangeStatsBridge.Kind.XZ_STAR,
        containedQuadCount,
        intersectQuadCount,
        0L,
        0L
      )
      result.asJava
    } else {
      XZStarSFC.setLastRangeStats(XZStarSFC.RangeStats(containedQuadCount, intersectQuadCount, 0L))
      RangeStatsBridge.setLast(
        RangeStatsBridge.Kind.XZ_STAR,
        containedQuadCount,
        intersectQuadCount,
        0L,
        0L
      )
      ranges
    }
  }
}

object XZStarSFC extends Serializable {
  case class QueryWindow(xmin: Double, ymin: Double, xmax: Double, ymax: Double)

  /**
    * Java 侧用于输出 VC 的“准确代价口径”
    * - visitedCells = containQuadCount + intersectQuadCount
    * - XZ_STAR 不使用 Redis 访问统计，redisAccessCount 固定为 0
    */
  case class RangeStats(containedQuadCount: Long, intersectQuadCount: Long, redisAccessCount: Long) extends Serializable

  private val lastStatsThreadLocal = new ThreadLocal[RangeStats]()

  def setLastRangeStats(stats: RangeStats): Unit = {
    lastStatsThreadLocal.set(stats)
  }

  def getLastVisitedCellsByContainIntersect(): Long = {
    val s = lastStatsThreadLocal.get()
    if (s == null) 0L else (s.containedQuadCount + s.intersectQuadCount)
  }

  def clearLastRangeStats(): Unit = {
    lastStatsThreadLocal.remove()
  }

  /**
   * 工厂方法：供 wrapper（XZStarIndex）创建核心实现。
   */
  def apply(g: Short, xBounds: (Double, Double), yBounds: (Double, Double), beta: Int): XZStarSFC =
    new XZStarSFC(g, xBounds, yBounds, beta)
}

