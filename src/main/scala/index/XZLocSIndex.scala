package index

import com.esri.core.geometry._
import org.apache.hadoop.hbase.util.Bytes
import org.locationtech.sfcurve.IndexRange
import redis.clients.jedis.resps.Tuple
import redis.clients.jedis.{Jedis, Response}

import java.util
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{Callable, Executors, Semaphore, TimeUnit}
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer


class XZLocSIndex(maxR: Short, xBounds: (Double, Double), yBounds: (Double, Double), alpha: Int, beta: Int) extends XZSFC(maxR, xBounds, yBounds, alpha, beta) with Serializable {

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

  def rangesThread(lng1: Double, lat1: Double, lng2: Double, lat2: Double, jedis: Jedis, indexTable: String, tspEncoding: Boolean): java.util.List[IndexRange] = {
    val queryWindow = QueryWindow(lng1, lat1, lng2, lat2)
    val ranges = new java.util.ArrayList[IndexRange](100)
    val remaining = new java.util.ArrayDeque[XZ2EE](300)
    val levelStop = XZ2EE(-1, -1, -1, -1, -1, -1)
    root.split()
    root.children.asScala.foreach(remaining.add)
    val pp = jedis.pipelined()
    remaining.add(levelStop)
    var level: Short = 1
    val executor = Executors.newCachedThreadPool()
    val callables = new util.HashSet[Callable[String]]
    val semaphore: Semaphore = new Semaphore(1)

    // 统计计数器
    val containedCount = new AtomicInteger(0)
    val intersectCount = new AtomicInteger(0)
    
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
      //      println(s"Visit quad: ${quad.elementCode}")
      //      println(s"Check quad ${quad.elementCode}, contained=${quad.isContained(queryWindow)}, insertion=${quad.insertion(queryWindow)}")
      if (quad.isContained(queryWindow)) {
        containedCount.incrementAndGet()
        val (min, max) = (quad.elementCode, quad.elementCode + IS(level))
        semaphore.acquire()
        ranges.add(IndexRange(min << (alpha * beta).toLong, (max << (alpha * beta).toLong) - 1L, contained = true))
        semaphore.release()
      } else if (quad.insertion(queryWindow)) {
        intersectCount.incrementAndGet()
        val key = quad.elementCode
        if (!tspEncoding) {
          if (null != quad.shapes) {
            if (quad.shapes.nonEmpty) {
              val signature = quad.insertSignature(queryWindow)
              for (elem <- quad.shapes) {
                if ((signature & elem) > 0) {
                  val min = elem | (key << (alpha * beta).toLong)
                  val range = IndexRange(min, min, contained = false)
                  ranges.add(range)
                  //flag = true
                }
              }
              //        } else if (!quad.isVisited()) {
            }
          }
        } else {
          if (null != quad.shapeMap) {
            if (!quad.shapeMap.isEmpty) {
              val signature = quad.insertSignature(queryWindow)
              for (elem <- quad.shapeMap.asScala) {
                if ((signature & elem._1) > 0) {
                  val min = elem._2.toLong | (key << (alpha * beta).toLong)
                  val range = IndexRange(min, min, contained = false)
                  ranges.add(range)
                }
              }
            }
            //        } else if (!quad.isVisited()) {
          }
        }
        if (null == quad.shapes && null == quad.shapeMap) {
          val resultTT = pp.zrangeByScoreWithScores(indexTable, key << (alpha * beta).toLong, ((key + 1L) << (alpha * beta).toLong) - 1L)
          callables.add(new SignatureCall(quad, key, resultTT, tspEncoding))
        }
        //        featureList.add(executor.submit(new SignatureTask(quad.elementCode, quad.insertSignature(queryWindow), resultTT)))
        if (level < maxR) {
          quad.split()
          quad.children.asScala.foreach(remaining.add)
        }
      }
    }

    //    pp.sync()
    pp.sync()
    //    pp.sync()
    //    pp.close()
    executor.invokeAll(callables)
    executor.shutdown()
    executor.awaitTermination(2000, TimeUnit.MILLISECONDS)
    pp.close()
    jedis.close()

    println(
      s"[XZLocSIndex.rangesThread] 覆盖 quad 数: ${containedCount.get()}, 相交 quad 数: ${intersectCount.get()}"
    )
    
    //    println(count.get())
    class SignatureCall(quad: XZ2EE, key: Long, resultT: Response[java.util.List[Tuple]], tspEncoding: Boolean) extends Callable[String] {
      override def call(): String = {
        val result = resultT.get
        //        quad.setVisited(true)
        if (null != result) {
          val shapeMap = new util.HashMap[Long, Int]()
          val indexedSet = new util.HashSet[Long]()
          if (!result.isEmpty) {
            val signature = quad.insertSignature(queryWindow)
            if (tspEncoding) {
              for (elem <- result.asScala) {
                //              val indexed = elem.getScore.toLong
                //              val s = indexed & (Math.pow(2, alpha * beta).toLong - 1L)
                //              indexedSet.add(s)
                val order = Bytes.toInt(elem.getBinaryElement, 4, 4) // shape order

                // =====================================================================
                // shape 实际上是 origin index，这里与 signature 相交只比较了低 alpha * beta 位
                // val originIndex = Bytes.toLong(elem.getBinaryElement, 8, 8)
                // val shapeId   = originIndex & ((1L << (alpha*beta)) - 1L)
                // ======================================================================
                val shape = Bytes.toLong(elem.getBinaryElement, 8, 8)
                indexedSet.add(shape)
                shapeMap.put(shape, order)
                if ((signature & shape) > 0) {
                  val min = order.toLong | (key << alpha * beta)
                  val range = IndexRange(min, min, contained = false)
                  semaphore.acquire()
                  ranges.add(range)
                  semaphore.release()
                  //flag = true
                }
              }
              quad.setShapeMap(shapeMap)
            } else {
              for (elem <- result.asScala) {
                val indexed = elem.getScore.toLong
                val s = indexed & (Math.pow(2, alpha * beta).toLong - 1L)
                indexedSet.add(s)
                if ((signature & s) > 0) {
                  val min = indexed
                  val range = IndexRange(min, min, contained = false)
                  semaphore.acquire()
                  ranges.add(range)
                  semaphore.release()
                }
              }
            }
            quad.setShapes(indexedSet.asScala.toList)
          }
        }
        key.toString
      }
    }
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

    // 统计计数器
    var containedCount = 0
    var intersectCount = 0
    
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
        containedCount += 1
        val (min, max) = (quad.elementCode, quad.elementCode + IS(level))
        ranges.add(IndexRange(min << (alpha * beta).toLong, (max << (alpha * beta).toLong) - 1L, contained = true))
      } else if (quad.insertion(queryWindow)) {
        intersectCount += 1
        val key = quad.elementCode
        //        val result = jedis.zrangeByScore(indexTable, key << (alpha*beta).toLong, ((key + 1) << (alpha*beta).toLong) - 1L)
        val result = jedis.zrangeByScoreWithScores(indexTable, key << (alpha * beta).toLong, ((key + 1L) << (alpha * beta).toLong) - 1L)
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
              val min = elem | (key << (alpha * beta).toLong)
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

    println(
      s"[XZLocSIndex.ranges] 覆盖 quad 数: $containedCount, 相交 quad 数: $intersectCount"
    )
    
    ranges
  }

  def ranges(lng1: Double, lat1: Double, lng2: Double, lat2: Double, indexMap: scala.collection.Map[Long, List[Long]]): java.util.List[IndexRange] = {
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
    var containedQuadCount = 0
    var intersectQuadCount = 0
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

    //TODO: 使用 redis 做缓存，并根据 key 快速查询包含的 key 和 signature
    def checkValue(quad: XZ2EE, level: Short): Unit = {
      if (quad.isContained(queryWindow)) {
        containedQuadCount += 1
        val (min, max) = (quad.elementCode, quad.elementCode + IS(level) - 1L)
        //println(quad.toString)
        ranges.add(IndexRange((min << (alpha * beta).toLong) | 0L, (max << (alpha * beta).toLong) | 0L, contained = true))
        size += 1
      } else if (quad.insertion(queryWindow)) {
        intersectQuadCount += 1
        //        checkList.add((quad, level))
        val key = quad.elementCode
        size += 1
        if (null == indexMap) {
          val signature = quad.insertSignature(queryWindow)
          for (i <- 0 until Math.pow(2, alpha * beta).toInt) {
            if ((signature | i) > 0) {
              val min = i.toLong | (key << (alpha * beta).toLong)
              val range = IndexRange(min, min, contained = false)
              ranges.add(range)
            }
          }
        } else {
          val indexSpaces = indexMap.get(key)
          if (null != quad.getShapes(indexMap)) {
            //var flag = false
            val signature = quad.insertSignature(queryWindow)
            for (elem <- indexSpaces.get) {
              if ((signature & elem) > 0) {
                val min = elem | (key << (alpha * beta).toLong)
                val range = IndexRange(min, min, contained = false)
                ranges.add(range)
                //flag = true
              }
            }
          }
        }
        //不用 indexedMap
        //        val signature = quad.insertSignature(queryWindow)
        //        for (i <- 0 until Math.pow(2, alpha * beta).toInt) {
        //          if ((signature | i) >= 0) {
        //            val min = i.toLong | (key << (alpha*beta).toLong)
        //            val range = IndexRange(min, min, contained = false)
        //            ranges.add(range)
        //            //flag = true
        //          }
        //        }
        //        executor.execute(new SignatureTask(quad.elementCode, quad.insertSignature(queryWindow)))
        if (level < maxR) {
          quad.split()
          quad.children.asScala.foreach(remaining.add)
        }
      }
    }

    //
    //    for (cElem <- checkList.asScala.sliding(100)) {
    //      executorService.execute(new Runnable {
    //        override def run(): Unit = {
    //          for (elem <- cElem) {
    //            val quad = elem._1
    //            val key = quad.elementCode
    //            val indexSpaces = indexMap.get(key)
    //            if (null != quad.getShapes(indexMap)) {
    //              //var flag = false
    //              val signature = quad.insertSignature(queryWindow)
    //              for (indexSpace <- indexSpaces.get) {
    //                if ((signature | indexSpace) >= 0) {
    //                  val min = indexSpace.toLong | (key << (alpha*beta).toLong)
    //                  val range = IndexRange(min, min, contained = false)
    //                  ranges.add(range)
    //                }
    //              }
    //            }
    //          }
    //        }
    //      });
    //    }
    //    executorService.shutdown();
    //    executorService.awaitTermination(200, TimeUnit.MILLISECONDS)

    //    executor.execute()
    //    executor.shutdown()
    //    executor.awaitTermination(10, TimeUnit.SECONDS)
    val factory = OperatorFactoryLocal.getInstance

    def rangesOfRelation(geometry: Geometry, operator: Operator.Type, indexMap: scala.collection.Map[Long, List[Long]]): java.util.List[IndexRange] = {
      val queryWindow = QueryWindow(lng1, lat1, lng2, lat2)
      val ranges = new java.util.ArrayList[IndexRange](100)
      val remaining = new java.util.ArrayDeque[XZ2EE](200)
      val levelStop = XZ2EE(-1, -1, -1, -1, -1, -1)
      root.split()
      root.children.asScala.foreach(remaining.add)
      remaining.add(levelStop)
      var level: Short = 1
      val op = factory.getOperator(operator).asInstanceOf[OperatorCrosses]
      //val result = op.execute(geometry1, geometry2, spatialReference, null) //val executor = Executors.newFixedThreadPool(8)
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
          val (min, max) = (quad.elementCode, quad.elementCode + IS(level) - 1L)
          ranges.add(IndexRange((min << (alpha * beta).toLong) | 0L, (max << (alpha * beta).toLong) | 0L, contained = true))
        } else if (quad.insertion(queryWindow)) {
          val key = quad.elementCode
          val indexSpaces = indexMap.get(key)
          if (null != quad.getShapes(indexMap)) {
            val signature = quad.insertSignature(queryWindow)
            for (elem <- indexSpaces.get) {
              if ((signature & elem) > 0) {
                val min = elem | (key << (alpha * beta).toLong)
                val range = IndexRange(min, min, contained = false)
                ranges.add(range)
              }
            }
          }
          //executor.execute(new SignatureTask(quad.elementCode, quad.insertSignature(queryWindow)))
          if (level < maxR) {
            quad.split()
            quad.children.asScala.foreach(remaining.add)
          }
        }
      }

      println(s"check size: $size")
      ranges
    }

    class SignatureTask(val key: Long, val signature: Int) extends Runnable {
      override def run(): Unit = {
        val indexSpaces = indexMap.get(key.toInt)
        if (indexSpaces.isDefined) {
          for (elem <- indexSpaces.get) {
            if ((signature & elem) > 0) {
              val min = key | (elem << 32)
              val range = IndexRange(min, min, contained = false)
              ranges.add(range)
            }
          }
        }
      }
    }
    println(
      s"[XZLocSIndex.ranges] 覆盖 quad 数: $containedQuadCount, 相交 quad 数: $intersectQuadCount"
    )
    println(s"checked: $size")
    ranges
  }
}

object XZLocSIndex extends Serializable {
  // the initial level of quads
  private val cache = new java.util.concurrent.ConcurrentHashMap[(String, Short, Int, Int), XZLocSIndex]()
  private val cacheBounds = new java.util.concurrent.ConcurrentHashMap[(String, Short, Int, Int, (Double, Double), (Double, Double)), XZLocSIndex]()

  def apply(g: Short, alpha: Int, beta: Int): XZLocSIndex = {
    var sfc = cache.get(("", g, alpha, beta))
    if (sfc == null) {
      sfc = new XZLocSIndex(g, (-180.0, 180.0), (-90.0, 90.0), alpha, beta)
      cache.put(("", g, alpha, beta), sfc)
    }
    sfc
  }

  def apply(g: Short, xBounds: (Double, Double), yBounds: (Double, Double), alpha: Int, beta: Int): XZLocSIndex = {
    var sfc = cacheBounds.get(("", g, alpha, beta, xBounds, yBounds))
    if (sfc == null) {
      sfc = new XZLocSIndex(g, xBounds, yBounds, alpha, beta)
      cacheBounds.put(("", g, alpha, beta, xBounds, yBounds), sfc)
    }
    sfc
  }

  def apply(table: String, g: Short, alpha: Int, beta: Int): XZLocSIndex = {
    var sfc = cache.get((table, g, alpha, beta))
    if (sfc == null) {
      sfc = new XZLocSIndex(g, (-180.0, 180.0), (-90.0, 90.0), alpha, beta)
      cache.put((table, g, alpha, beta), sfc)
    }
    sfc
  }

  def apply(table: String, g: Short, xBounds: (Double, Double), yBounds: (Double, Double), alpha: Int, beta: Int): XZLocSIndex = {
    var sfc = cacheBounds.get((table, g, alpha, beta, xBounds, yBounds))
    if (sfc == null) {
      sfc = new XZLocSIndex(g, xBounds, yBounds, alpha, beta)
      cacheBounds.put((table, g, alpha, beta, xBounds, yBounds), sfc)
    }
    sfc
  }
}