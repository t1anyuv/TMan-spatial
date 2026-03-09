package index

import com.esri.core.geometry._
import org.locationtech.sfcurve.IndexRange
import redis.clients.jedis.Jedis

import java.util
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer


class XZIndex(maxR: Short, xBounds: (Double, Double), yBounds: (Double, Double), alpha: Int, beta: Int) extends XZSFC(maxR, xBounds, yBounds, alpha, beta) with Serializable {

  val root: XZ2EE = XZ2EE(xBounds._1, yBounds._1, xBounds._2, yBounds._2, 0, 0L)

  override def signature(x: Double, y: Double, w: Double, h: Double, geometry: Geometry): Long = {
    var signature = 0L
    //    for (i <- 0 until ((alpha * beta) / 8 + 1)) {
    //      signature(i) = 0
    //    }
    val cellW = 2.0 * w / alpha.toDouble
    val cellH = 2.0 * h / beta.toDouble
    for (i <- 0 until alpha) {
      for (j <- 0 until beta) {
        val minX = x + cellW * i
        val minY = y + cellH * j
        val env = new Envelope(minX, minY, minX + cellW, minY + cellH)
        //env.in
        if (OperatorIntersects.local().execute(env, geometry, SpatialReference.create(4326), null)) {
          signature |= (1L << (i * beta + j).toLong)
        }
      }
    }
    //    if (signature == 0) {
    //      println()
    //    }
    signature
  }

  override def index(geometry: Geometry, lenient: Boolean = false): (Int, Long, Long) = {
    val mbr = new Envelope2D()
    geometry.queryEnvelope2D(mbr)
    val l = resolution(geometry, 2, 2)
    index(geometry, l)
  }

  //  def ranges(lat1: Double, lng1: Double, lat2: Double, lng2: Double): java.util.List[IndexRange] = {
  //
  //  }


  def rangesThread(lng1: Double, lat1: Double, lng2: Double, lat2: Double, jedis: Jedis, indexTable: String, tspEncoding: Boolean = false): java.util.List[IndexRange] = {
    val queryWindow = QueryWindow(lng1, lat1, lng2, lat2)
    val ranges = new java.util.ArrayList[IndexRange](100)
    val remaining = new java.util.ArrayDeque[XZ2EE](300)
    val levelStop = XZ2EE(-1, -1, -1, -1, -1, -1)
    root.split()
    root.children.asScala.foreach(remaining.add)
    remaining.add(levelStop)
    var level: Short = 1
    //    var count = new AtomicLong(0)
    while (!remaining.isEmpty) {
      val next = remaining.poll
      if (next.eq(levelStop)) {
        // we've fully processed a level, increment our state
        if (!remaining.isEmpty && level < maxR) {
          level = (level + 1).toShort
          remaining.add(levelStop)
        }
      } else {
        checkValue(next, level)
      }
    }

    def checkValue(quad: XZ2EE, level: Short): Unit = {
      if (quad.isContained(queryWindow)) {
        val (min, max) = (quad.elementCode, quad.elementCode + IS(level))
        ranges.add(IndexRange(min, max - 1L, contained = true))
      } else if (quad.insertion(queryWindow)) {
        val key = quad.elementCode
        ranges.add(IndexRange(key, key, contained = false))
        //        featureList.add(executor.submit(new SignatureTask(quad.elementCode, quad.insertSignature(queryWindow), resultTT)))
        if (level < maxR) {
          quad.split()
          quad.children.asScala.foreach(remaining.add)
        }
      }
    }
    //    println(count.get())
    if (ranges.size() > 1) {
      //      ranges.asScala
      ranges.sort(IndexRange.IndexRangeIsOrdered)
      var current = ranges.get(0) // note: should always be at least one range
      val result = ArrayBuffer.empty[IndexRange]
      var i = 1
      while (i < ranges.size()) {
        val range = ranges.get(i)
        if (range.lower <= current.upper + 1) {
          current = IndexRange(current.lower, math.max(current.upper, range.upper), current.contained && range.contained)
        } else {
          result.append(current)
          current = range
        }
        i += 1
      }
      result.append(current)
      result.asJava
    } else {
      ranges
    }
  }

  def ranges(lng1: Double, lat1: Double, lng2: Double, lat2: Double, jedis: Jedis, indexTable: String): java.util.List[IndexRange] = {
    val queryWindow = QueryWindow(lng1, lat1, lng2, lat2)
    val ranges = new java.util.ArrayList[IndexRange](100)
    val checkList = new java.util.ArrayList[(XZ2EE, Short)](300)
    val remaining = new java.util.ArrayDeque[XZ2EE](200)
    val levelStop = XZ2EE(-1, -1, -1, -1, -1, -1)
    root.split()
    root.children.asScala.foreach(remaining.add)
    remaining.add(levelStop)
    var level: Short = 1
    //    val executorService = Executors.newCachedThreadPool
    //    val executor = Executors.newFixedThreadPool(8)
    var size = 0
    while (!remaining.isEmpty) {
      val next = remaining.poll
      if (next.eq(levelStop)) {
        // we've fully processed a level, increment our state
        if (!remaining.isEmpty && level < maxR) {
          level = (level + 1).toShort
          remaining.add(levelStop)
        }
      } else {
        checkValue(next, level)
      }
    }

    def checkValue(quad: XZ2EE, level: Short): Unit = {
      if (quad.isContained(queryWindow)) {
        val (min, max) = (quad.elementCode, quad.elementCode + IS(level))
        ranges.add(IndexRange(min << (alpha * beta).toLong, (max << (alpha * beta).toLong) - 1L, contained = true))
      } else if (quad.insertion(queryWindow)) {
        val key = quad.elementCode
        val finalKey = key
        //        val result = jedis.zrangeByScore(indexTable, key << (alpha*beta).toLong, ((key + 1) << (alpha*beta).toLong) - 1L)
        val result = jedis.zrangeByScoreWithScores(indexTable, finalKey << (alpha * beta).toLong, ((finalKey + 1L) << (alpha * beta).toLong) - 1L)
        // println(s"${key << (alpha*beta).toLong}   ${((key + 1) << (alpha*beta).toLong) - 1L}")
        if (null != result) {
          val signature = quad.insertSignature(queryWindow)

          //          for (i <- 0 until Math.pow(2, alpha * beta).toInt) {
          //            if ((signature | i) > 0) {
          //              val min = i.toLong | (key << (alpha * beta).toLong)
          //              val range = IndexRange(min, min, contained = false)
          //              ranges.add(range)
          //            }
          //          }
          val indexedSet = new util.HashSet[Long]()
          for (elem <- result.asScala) {
            indexedSet.add(elem.getScore.toLong)
          }
          for (indexed <- indexedSet.asScala) {
            val elem = indexed & (Math.pow(2, alpha * beta).toLong - 1L)
            if ((signature & elem) > 0) {
              val min = elem | (finalKey << (alpha * beta).toLong)
              val range = IndexRange(min, min, contained = false)
              ranges.add(range)
              //flag = true
            }
          }
          //        executor.execute(new SignatureTask(quad.elementCode, quad.insertSignature(queryWindow)))
        }
        if (level < maxR) {
          quad.split()
          quad.children.asScala.foreach(remaining.add)
        }
      }
    }

    jedis.close()
    ranges
  }

  def ranges(lng1: Double, lat1: Double, lng2: Double, lat2: Double): java.util.List[IndexRange] = {
    val queryWindow = QueryWindow(lng1, lat1, lng2, lat2)
    val ranges = new java.util.ArrayList[IndexRange](100)
    val checkList = new java.util.ArrayList[(XZ2EE, Short)](300)
    val remaining = new java.util.ArrayDeque[XZ2EE](200)
    val levelStop = XZ2EE(-1, -1, -1, -1, -1, -1)
    root.split()
    root.children.asScala.foreach(remaining.add)
    remaining.add(levelStop)
    var level: Short = 1


    //    val executorService = Executors.newCachedThreadPool
    //    val executor = Executors.newFixedThreadPool(8)
    var size = 0
    while (!remaining.isEmpty) {
      val next = remaining.poll
      if (next.eq(levelStop)) {
        // we've fully processed a level, increment our state
        if (!remaining.isEmpty && level < maxR) {
          level = (level + 1).toShort
          remaining.add(levelStop)
        }
      } else {
        checkValue(next, level)
      }
    }

    //TODO 使用redis做缓存，并根据key快速查询包含的key和signature
    def checkValue(quad: XZ2EE, level: Short): Unit = {
      if (quad.isContained(queryWindow)) {
        val (min, max) = (quad.elementCode, quad.elementCode + IS(level) - 1L)
        //println(quad.toString)
        ranges.add(IndexRange(min , max, contained = true))
        size += 1
      } else if (quad.insertion(queryWindow)) {
        //        checkList.add((quad, level))
        val key = quad.elementCode
        size += 1
        ranges.add(IndexRange(key, key, contained = false))
        if (level < maxR) {
          quad.split()
          quad.children.asScala.foreach(remaining.add)
        }
      }
    }

    println(s"checked: $size")
    if (ranges.size() > 1) {
      //      ranges.asScala
      ranges.sort(IndexRange.IndexRangeIsOrdered)
      var current = ranges.get(0) // note: should always be at least one range
      val result = ArrayBuffer.empty[IndexRange]
      var i = 1
      while (i < ranges.size()) {
        val range = ranges.get(i)
        if (range.lower <= current.upper + 1) {
          current = IndexRange(current.lower, math.max(current.upper, range.upper), current.contained && range.contained)
        } else {
          result.append(current)
          current = range
        }
        i += 1
      }
      result.append(current)
      result.asJava
    } else {
      ranges
    }
  }

}

object XZIndex extends Serializable {
  // the initial level of quads
  private val cache = new java.util.concurrent.ConcurrentHashMap[(String, Short, Int, Int), XZIndex]()
  private val cacheBounds = new java.util.concurrent.ConcurrentHashMap[(String, Short, Int, Int, (Double, Double), (Double, Double)), XZIndex]()

  def apply(g: Short, alpha: Int, beta: Int): XZIndex = {
    var sfc = cache.get(("", g, alpha, beta))
    if (sfc == null) {
      sfc = new XZIndex(g, (-180.0, 180.0), (-90.0, 90.0), alpha, beta)
      cache.put(("", g, alpha, beta), sfc)
    }
    sfc
  }

  def apply(g: Short, xBounds: (Double, Double), yBounds: (Double, Double), alpha: Int, beta: Int): XZIndex = {
    var sfc = cacheBounds.get(("", g, alpha, beta, xBounds, yBounds))
    if (sfc == null) {
      sfc = new XZIndex(g, xBounds, yBounds, alpha, beta)
      cacheBounds.put(("", g, alpha, beta, xBounds, yBounds), sfc)
    }
    sfc
  }

  def apply(table: String, g: Short, alpha: Int, beta: Int): XZIndex = {
    var sfc = cache.get((table, g, alpha, beta))
    if (sfc == null) {
      sfc = new XZIndex(g, (-180.0, 180.0), (-90.0, 90.0), alpha, beta)
      cache.put((table, g, alpha, beta), sfc)
    }
    sfc
  }

  def apply(table: String, g: Short, xBounds: (Double, Double), yBounds: (Double, Double), alpha: Int, beta: Int): XZIndex = {
    var sfc = cacheBounds.get((table, g, alpha, beta, xBounds, yBounds))
    if (sfc == null) {
      sfc = new XZIndex(g, xBounds, yBounds, alpha, beta)
      cacheBounds.put((table, g, alpha, beta, xBounds, yBounds), sfc)
    }
    sfc
  }
}