package index

import org.locationtech.sfcurve.IndexRange

import scala.collection.JavaConverters.asScalaBufferConverter

class CellIndex(maxR: Short, xBounds: (Double, Double), yBounds: (Double, Double), alpha: Int, beta: Int) extends XZSFC(maxR, xBounds, yBounds, alpha, beta) with Serializable {

  val root: TShapeEE = TShapeEE(xBounds._1, yBounds._1, xBounds._2, yBounds._2, 0, 0L)

  def ranges(lng1: Double, lat1: Double, lng2: Double, lat2: Double): java.util.List[IndexRange] = {
    val queryWindow = QueryWindow(lng1, lat1, lng2, lat2)
    val ranges = new java.util.ArrayList[IndexRange](100)
    val remaining = new java.util.ArrayDeque[TShapeEE](200)
    val levelStop = TShapeEE(-1, -1, -1, -1, -1, -1)
    root.split()
    root.children.asScala.foreach(remaining.add)
    remaining.add(levelStop)
    var level: Short = 1
    while (!remaining.isEmpty) {
      val next = remaining.poll
      if (next.eq(levelStop)) {
        if (!remaining.isEmpty && level < maxR) {
          level = (level + 1).toShort
          remaining.add(levelStop)
        }
      } else {
        checkValue(next, level)
      }
    }

    def checkValue(quad: TShapeEE, level: Short): Unit = {
      if (quad.isContained(queryWindow)) {
        val (min, max) = (quad.elementCode, quad.elementCode + IS(level))
        ranges.add(IndexRange(min << 8L, (max << 8L) - 1L, contained = true))
//        println(s"${level} e: ${quad.elementCode}, min: ${min << 8L}, max:${(max << 8L) - 1L}")
      } else if (quad.insertion(queryWindow)) {
        val signature = quad.insertSignature(queryWindow)
        val key = quad.elementCode
        for (i <- 0 until alpha) {
          for (j <- 0 until beta) {
            val shape = 1L << (i * alpha + j).toLong
            if ((shape & signature) > 0) {
              val index = (key << 8L) | (i * alpha + j)
              ranges.add(IndexRange(index, index, contained = true))
//              println(s"r: ${level} e: ${key}, min: ${index}, max:$index")
            }
          }
        }
        if (level < maxR) {
          quad.split()
          quad.children.asScala.foreach(remaining.add)
        }
      }
    }

    ranges
  }
}

object CellIndex extends Serializable {
  // the initial level of quads
  private val cache = new java.util.concurrent.ConcurrentHashMap[(String, Short, Int, Int), CellIndex]()
  private val cacheBounds = new java.util.concurrent.ConcurrentHashMap[(String, Short, Int, Int, (Double, Double), (Double, Double)), CellIndex]()

  def apply(g: Short, alpha: Int, beta: Int): CellIndex = {
    var sfc = cache.get(("", g, alpha, beta))
    if (sfc == null) {
      sfc = new CellIndex(g, (-180.0, 180.0), (-90.0, 90.0), alpha, beta)
      cache.put(("", g, alpha, beta), sfc)
    }
    sfc
  }

  def apply(g: Short, xBounds: (Double, Double), yBounds: (Double, Double), alpha: Int, beta: Int): CellIndex = {
    var sfc = cacheBounds.get(("", g, alpha, beta, xBounds, yBounds))
    if (sfc == null) {
      sfc = new CellIndex(g, xBounds, yBounds, alpha, beta)
      cacheBounds.put(("", g, alpha, beta, xBounds, yBounds), sfc)
    }
    sfc
  }

  def apply(table: String, g: Short, alpha: Int, beta: Int): CellIndex = {
    var sfc = cache.get((table, g, alpha, beta))
    if (sfc == null) {
      sfc = new CellIndex(g, (-180.0, 180.0), (-90.0, 90.0), alpha, beta)
      cache.put((table, g, alpha, beta), sfc)
    }
    sfc
  }

  def apply(table: String, g: Short, xBounds: (Double, Double), yBounds: (Double, Double), alpha: Int, beta: Int): CellIndex = {
    var sfc = cacheBounds.get((table, g, alpha, beta, xBounds, yBounds))
    if (sfc == null) {
      sfc = new CellIndex(g, xBounds, yBounds, alpha, beta)
      cacheBounds.put((table, g, alpha, beta, xBounds, yBounds), sfc)
    }
    sfc
  }
}