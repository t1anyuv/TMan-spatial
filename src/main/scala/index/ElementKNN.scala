package index

import org.locationtech.jts.geom.PrecisionModel
import org.locationtech.sfcurve.IndexRange

import java.util
import scala.collection.JavaConverters._

/**
 * XZStarSFC 的“候选区间生成”用元素（这里我们只需要用于 SRQ 矩形 ranges）。
 *
 * 说明：当前工程中其它索引（LocS / XZLoc / LETI）用于空间窗口的逻辑是分层四叉树 + IndexRange。
 * XZ_STAR 的实现复用了论文里的元素编码逻辑：elementCode / IS(level) + insertCodes。
 */
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
  // 这里只保留 SRQ ranges 需要的状态/方法，KNN/相似度扩展在后续阶段再补。

  val xLength: Double = xmax - xmin
  val yLength: Double = ymax - ymin

  val positionIndex: Array[Int] = Array(3, 5, 7, 15, 11, 13, 14, 9, 6, 1)

  val children: util.ArrayList[ElementKNN] = new util.ArrayList[ElementKNN](4)

  def insertion(window: XZStarSFC.QueryWindow): Boolean = {
    // enlarge element by one cell (xLength/yLength) in X/Y, matching the original XZStarSFC logic
    window.xmax >= xmin && window.ymax >= ymin && window.xmin <= xmax + xLength && window.ymin <= ymax + yLength
  }

  def insertion(window: XZStarSFC.QueryWindow, x1: Double, y1: Double, x2: Double, y2: Double): Boolean = {
    window.xmax >= x1 && window.ymax >= y1 && window.xmin <= x2 && window.ymin <= y2
  }

  def isContained(window: XZStarSFC.QueryWindow): Boolean = {
    window.xmin <= xmin && window.ymin <= ymin && window.xmax >= xmax + xLength && window.ymax >= ymax + yLength
  }

  /**
   * 将一个“intersect 但不包含”的元素，映射到多个离散的 IndexRange（contained=false）。
   */
  def insertCodes(window: XZStarSFC.QueryWindow): util.List[IndexRange] = {
    var pos = 0
    val xeMax = xmax + xLength
    val yeMax = ymax + yLength
    val xCenter = xmax
    val yCenter = ymax

    if (insertion(window, xmin, ymin, xCenter, yCenter)) pos |= 1
    if (insertion(window, xCenter, ymin, xeMax, yCenter)) pos |= 2
    if (insertion(window, xmin, yCenter, xCenter, yeMax)) pos |= 4
    if (insertion(window, xCenter, yCenter, xeMax, yeMax)) pos |= 8

    val results = new util.ArrayList[Long](8)

    // level<g：只用 0..8；level==g：用 0..9
    var pSize = 9L
    if (level < g) pSize = 8L

    var i = 0L
    while (i <= pSize) {
      if ((positionIndex(i.toInt) & pos) != 0) {
        results.add(i + 1L)
      }
      i += 1L
    }

    results
      .asScala
      .map { v =>
        IndexRange(v + elementCode - 10L, v + elementCode - 10L, contained = false)
      }
      .asJava
  }

  def IS(i: Int): Long = {
    (39L * math.pow(4, g - i).toLong - 9L) / 3L
  }

  /**
   * 标准四叉树拆分，elementCode 的偏移规则保持与原实现一致。
   */
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
}

