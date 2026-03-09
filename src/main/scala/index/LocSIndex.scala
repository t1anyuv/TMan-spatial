package index

import entity.Trajectory
import org.apache.hadoop.hbase.util.Bytes
import org.locationtech.sfcurve.IndexRange
import redis.clients.jedis.resps.Tuple
import redis.clients.jedis.{Jedis, Response}

import java.util
import java.util.concurrent.{Callable, Executors, Semaphore, TimeUnit}
import scala.collection.mutable.ArrayBuffer


class LocSIndex(maxR: Short, xBounds: (Double, Double), yBounds: (Double, Double), alpha: Int, beta: Int) extends XZSFC(maxR, xBounds, yBounds, alpha, beta) with Serializable {

  val root: TShapeEE = TShapeEE(xBounds._1, yBounds._1, xBounds._2, yBounds._2, 0, 0L)

  import scala.collection.JavaConverters._

  def similarityRanges(searTraj: Trajectory, dis: Double, jedis: Jedis, indexTable: String, tspEncoding: Boolean = false): java.util.List[IndexRange] = {
    val ranges = new java.util.ArrayList[IndexRange](100)
    val boundary1 = searTraj.getMultiPoint.getEnvelopeInternal
    val boundaryEnv = searTraj.getMultiPoint.getEnvelopeInternal

    boundary1.expandBy(dis)
    val remaining = new java.util.ArrayDeque[TShapeEE](200)
    val minimumResolution = resolutionMBR(boundary1.getMinX, boundary1.getMinY, boundary1.getMaxX, boundary1.getMaxY, alpha, beta)
    var maximumResolution = minimumResolution.length
    var currXS = minimumResolution.xWidth / 2.0
    var currYS = minimumResolution.yWidth / 2.0
    //优化
    while ((boundaryEnv.getWidth - currXS) / 2.0 < dis && (boundaryEnv.getHeight - currYS) / 2.0 < dis && maximumResolution < maxR) {
      maximumResolution += 1
      currXS /= 2.0
      currYS /= 2.0
    }
    val spoint = searTraj.getGeometryN(0)
    val epoint = searTraj.getGeometryN(searTraj.getNumGeometries - 1)
    var level = minimumResolution.length
    val levelStop = TShapeEE(-1, -1, -1, -1, -1, -1)
    remaining.add(root.search(root, minimumResolution.xTrue, minimumResolution.yTrue, minimumResolution.length))
    remaining.add(root.search(root, minimumResolution.xTrue + minimumResolution.xWidth, minimumResolution.yTrue, minimumResolution.length))
    remaining.add(root.search(root, minimumResolution.xTrue, minimumResolution.yTrue + minimumResolution.yWidth, minimumResolution.length))
    remaining.add(root.search(root, minimumResolution.xTrue + minimumResolution.xWidth, minimumResolution.yTrue + minimumResolution.yWidth, minimumResolution.length))
    remaining.add(levelStop)

    while (!remaining.isEmpty) {
      val next = remaining.poll
      if (next == levelStop && !remaining.isEmpty && level < maximumResolution) {
        remaining.add(levelStop)
        level = level + 1
      } else {
        if (next.neededToCheck(boundaryEnv, dis)) {
          val key = next.elementCode
          if (null == next.shapes) {
            loadQuadShapes(jedis, indexTable, tspEncoding, next, key)
          }
          if (next.shapes.nonEmpty) {
            val candidates = next.checkPositionCode(searTraj, dis, spoint, epoint)
            if (null != candidates) {
              ranges.addAll(candidates)
            }
          }
          if (level < maximumResolution) {
            //next.getChildren.asScala
            next.getChildren.asScala.foreach(v => {
              remaining.add(v)
            })
          }
        }
      }
    }
    ranges
  }

  private def loadQuadShapes(jedis: Jedis, indexTable: String, tspEncoding: Boolean, next: TShapeEE, key: Long): Unit = {
    val result = jedis.zrangeByScoreWithScores(indexTable, key << (alpha * beta).toLong, ((key + 1L) << (alpha * beta).toLong) - 1L)
    if (null != result) {
      val shapeMap = new util.HashMap[Long, Int]()
      val indexedSet = new util.HashSet[Long]()
      if (!result.isEmpty) {
        if (tspEncoding) {
          for (elem <- result.asScala) {
            val order = Bytes.toInt(elem.getBinaryElement, 4, 4)
            var shape = Bytes.toLong(elem.getBinaryElement, 8, 8)
            shape = shape & (Math.pow(2, alpha * beta).toLong - 1L)
            indexedSet.add(shape)
            shapeMap.put(shape, order)
          }
          next.setShapeMap(shapeMap)
        } else {
          for (elem <- result.asScala) {
            val indexed = elem.getScore.toLong
            val s = indexed & (Math.pow(2, alpha * beta).toLong - 1L)
            indexedSet.add(s)
          }
        }
        next.setShapes(indexedSet.asScala.toList)
      }
    }
  }

  def rangesThread(lng1: Double, lat1: Double, lng2: Double, lat2: Double, jedis: Jedis, indexTable: String, tspEncoding: Boolean = false): java.util.List[IndexRange] = {
    val queryWindow = QueryWindow(lng1, lat1, lng2, lat2)
    val ranges = new java.util.ArrayList[IndexRange](100)
    val remaining = new java.util.ArrayDeque[TShapeEE](300)
    val levelStop = TShapeEE(-1, -1, -1, -1, -1, -1)
    root.split()
    root.children.asScala.foreach(remaining.add)
    val pp = jedis.pipelined()
    remaining.add(levelStop)
    var level: Short = 1
    val executor = Executors.newCachedThreadPool()
    val callables = new util.HashSet[Callable[String]]
    val semaphore: Semaphore = new Semaphore(1)
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

    def checkValue(quad: TShapeEE, level: Short): Unit = {
      if (quad.isContained(queryWindow)) {
        val (min, max) = (quad.elementCode, quad.elementCode + IS(level))
        semaphore.acquire()
        ranges.add(IndexRange(min << (alpha * beta).toLong, (max << (alpha * beta).toLong) - 1L, contained = true))
        semaphore.release()
      } else if (quad.insertion(queryWindow)) {
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
    pp.sync()
    //    pp.close()
    executor.invokeAll(callables)
    executor.shutdown()
    executor.awaitTermination(2000, TimeUnit.MILLISECONDS)
    pp.close()
    jedis.close()

    class SignatureCall(quad: TShapeEE, key: Long, resultT: Response[java.util.List[Tuple]], tspEncoding: Boolean) extends Callable[String] {
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
                val order = Bytes.toInt(elem.getBinaryElement, 4, 4)
                var shape = Bytes.toLong(elem.getBinaryElement, 8, 8)
                shape = shape & (Math.pow(2, alpha * beta).toLong - 1L)
                //                println(s"$key, $order, $shape")
                indexedSet.add(shape)
                shapeMap.put(shape, order)
                if ((signature & shape) > 0) {
                  val min = order.toLong | (key << alpha * beta) // key 已经是 finalKey（qOrder）
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
    //    ranges
  }

  def ranges(lng1: Double, lat1: Double, lng2: Double, lat2: Double, jedis: Jedis, indexTable: String): java.util.List[IndexRange] = {
    val queryWindow = QueryWindow(lng1, lat1, lng2, lat2)
    val ranges = new java.util.ArrayList[IndexRange](100)
    val checkList = new java.util.ArrayList[(TShapeEE, Short)](300)
    val remaining = new java.util.ArrayDeque[TShapeEE](200)
    val levelStop = TShapeEE(-1, -1, -1, -1, -1, -1)
    root.split()
    root.children.asScala.foreach(remaining.add)
    remaining.add(levelStop)
    var level: Short = 1
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

    def checkValue(quad: TShapeEE, level: Short): Unit = {
      if (quad.isContained(queryWindow)) {
        val (min, max) = (quad.elementCode, quad.elementCode + IS(level))
        ranges.add(IndexRange(min << (alpha * beta).toLong, (max << (alpha * beta).toLong) - 1L, contained = true))
      } else if (quad.insertion(queryWindow)) {
        val key = quad.elementCode
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
    ranges
  }


  def ranges(lng1: Double, lat1: Double, lng2: Double, lat2: Double, indexMap: scala.collection.Map[Long, List[Long]]): java.util.List[IndexRange] = {
    val queryWindow = QueryWindow(lng1, lat1, lng2, lat2)
    val ranges = new java.util.ArrayList[IndexRange](100)
    val checkList = new java.util.ArrayList[(TShapeEE, Short)](300)
    val remaining = new java.util.ArrayDeque[TShapeEE](200)
    val levelStop = TShapeEE(-1, -1, -1, -1, -1, -1)
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

    //TODO: 使用redis做缓存，并根据 key 快速查询包含的 key 和 signature
    def checkValue(quad: TShapeEE, level: Short): Unit = {
      if (quad.isContained(queryWindow)) {
        val (min, max) = (quad.elementCode, quad.elementCode + IS(level) - 1L)
        //println(quad.toString)
        ranges.add(IndexRange(min << (alpha * beta).toLong, (max << (alpha * beta).toLong) - 1L, contained = true))
        size += 1
      } else if (quad.insertion(queryWindow)) {
        //        checkList.add((quad, level))
        val key = quad.elementCode
        size += 1
        if (null == indexMap) {
          val signature = quad.insertSignature(queryWindow)
          for (i <- 0 until Math.pow(2, alpha * beta).toInt) {
            if ((signature & i) > 0) {
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
        // 不用indexedMap
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
    //            if (null != quad.loadQuadShapes(indexMap)) {
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

    println(s"checked: $size")
    ranges
  }
}

object LocSIndex extends Serializable {
  // the initial level of quads
  private val cache = new java.util.concurrent.ConcurrentHashMap[(String, Short, Int, Int), LocSIndex]()
  private val cacheBounds = new java.util.concurrent.ConcurrentHashMap[(String, Short, Int, Int, (Double, Double), (Double, Double)), LocSIndex]()

  def apply(g: Short, alpha: Int, beta: Int): LocSIndex = {
    var sfc = cache.get(("", g, alpha, beta))
    if (sfc == null) {
      sfc = new LocSIndex(g, (-180.0, 180.0), (-90.0, 90.0), alpha, beta)
      cache.put(("", g, alpha, beta), sfc)
    }
    sfc
  }

  def apply(g: Short, xBounds: (Double, Double), yBounds: (Double, Double), alpha: Int, beta: Int): LocSIndex = {
    var sfc = cacheBounds.get(("", g, alpha, beta, xBounds, yBounds))
    if (sfc == null) {
      sfc = new LocSIndex(g, xBounds, yBounds, alpha, beta)
      cacheBounds.put(("", g, alpha, beta, xBounds, yBounds), sfc)
    }
    sfc
  }

  def apply(table: String, g: Short, alpha: Int, beta: Int): LocSIndex = {
    var sfc = cache.get((table, g, alpha, beta))
    if (sfc == null) {
      sfc = new LocSIndex(g, (-180.0, 180.0), (-90.0, 90.0), alpha, beta)
      cache.put((table, g, alpha, beta), sfc)
    }
    sfc
  }

  def apply(table: String, g: Short, xBounds: (Double, Double), yBounds: (Double, Double), alpha: Int, beta: Int): LocSIndex = {
    var sfc = cacheBounds.get((table, g, alpha, beta, xBounds, yBounds))
    if (sfc == null) {
      sfc = new LocSIndex(g, xBounds, yBounds, alpha, beta)
      cacheBounds.put((table, g, alpha, beta, xBounds, yBounds), sfc)
    }
    sfc
  }
}