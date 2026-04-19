package index

import org.apache.hadoop.hbase.util.Bytes
import org.locationtech.sfcurve.IndexRange
import redis.clients.jedis.Jedis
import utils.LetiOrderResolver
import utils.LSFCReader
import utils.LSFCReader.ParentQuad

import java.{lang, util}
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

class LETILocSIndex(
                     maxR: Short,
                     xBounds: (Double, Double),
                     yBounds: (Double, Double),
                     alpha: Int,
                     beta: Int,
                     adaptivePartition: Boolean = false,
                     orderDefinitionPath: String = LetiOrderResolver.DEFAULT_CANONICAL_PATH
                   ) extends LocSIndex(maxR, xBounds, yBounds, alpha, beta)
  with Serializable {

  private val mapper: LSFCReader.LSFCMapper =
    LSFCReader.loadFromClasspath(LetiOrderResolver.resolveClasspathResource(orderDefinitionPath))

  private val quadCodeOrder: util.Map[lang.Long, Integer] = mapper.quadCodeOrder
  private val quadCodeParentQuad: util.Map[lang.Long, ParentQuad] = mapper.quadCodeParentQuad
  private val validQuadCodes: util.Set[lang.Long] = mapper.validQuadCodes

  private case class QOrderTreeNode(
                                     quadCode: Long,
                                     qOrder: Int,
                                     parentQuad: ParentQuad,
                                     level: Int,
                                     subtreeUpper: Long,
                                     eeXmin: Double,
                                     eeYmin: Double,
                                     eeXmax: Double,
                                     eeYmax: Double,
                                     partitionAlpha: Int,
                                     partitionBeta: Int,
                                     minScore: Long,
                                     maxScore: Long,
                                     children: java.util.ArrayList[QOrderTreeNode] = new java.util.ArrayList[QOrderTreeNode](),
                                     var containedIntervals: LETILocSIndex.ContainedIntervals = LETILocSIndex.ContainedIntervals(
                                       new java.util.ArrayList[IndexRange](),
                                       new java.util.ArrayList[IndexRange]()
                                     )
                                   )

  private case class PendingIntersect(
                                       qOrder: Int,
                                       signature: Long
                                     )

  private case class IntersectBatch(
                                     startQOrder: Int,
                                     endQOrder: Int,
                                     signatureByQOrder: java.util.HashMap[lang.Integer, lang.Long],
                                     cacheKey: String,
                                     minScore: Long,
                                     maxScore: Long,
                                     var cachedTuples: java.util.List[redis.clients.jedis.resps.Tuple] = null,
                                     var response: redis.clients.jedis.Response[_ <: java.util.Collection[
                                       redis.clients.jedis.resps.Tuple
                                     ]] = null
                                   )

  /**
   * 最大形状位数，用于统一编码空间
   * 自适应划分时从 order metadata 获取，否则使用全局 alpha * beta
   */
  private val redisShapeMoveBits: Int =
    if (mapper.metadata != null && mapper.metadata.maxPartition > 0) mapper.metadata.maxPartition
    else alpha * beta

  private val supportsContiguousSubtreeOrders: Boolean =
    mapper.metadata != null && mapper.metadata.contiguousSubtreeOrders

  /**
   * 形状编码的位数
   * 自适应划分时使用 maxShapeBits，否则使用全局 alpha * beta
   */
  private val mainTableMoveBits: Int =
    if (mapper.metadata != null && mapper.metadata.maxShapes > 0L) {
      math.max(1, 64 - java.lang.Long.numberOfLeadingZeros(mapper.metadata.maxShapes))
    } else 1

  private val qOrderRoots: java.util.List[QOrderTreeNode] = buildQOrderTree()

  /**
   * 计算查询窗口对应的索引范围
   *
   * @param lng1       查询窗口左下角经度
   * @param lat1       查询窗口左下角纬度
   * @param lng2       查询窗口右上角经度
   * @param lat2       查询窗口右上角纬度
   * @param jedis      Redis 连接
   * @param indexTable Redis 索引表名
   * @return 索引范围列表
   */
  override def ranges(
                       lng1: Double,
                       lat1: Double,
                       lng2: Double,
                       lat2: Double,
                       jedis: Jedis,
                       indexTable: String
                     ): java.util.List[IndexRange] = {
    rangesWithNativeQOrderTree(lng1, lat1, lng2, lat2, jedis, indexTable)
  }

  def rangesWithNativeQOrderTree(
                                  lng1: Double,
                                  lat1: Double,
                                  lng2: Double,
                                  lat2: Double,
                                  jedis: Jedis,
                                  indexTable: String
                                ): java.util.List[IndexRange] = {
    val debug = java.lang.Boolean.getBoolean("tman.debug.leti")
    val debugLimit = 20

    var printedIntersect = 0
    var printedResult = 0
    var redisTupleTotal: Long = 0L
    var redisTuplePassed: Long = 0L
    var redisAccessCount: Long = 0L
    val unionMasks = LETILocSIndex.getCachedUnionMasks(jedis, indexTable)

    val queryWindow = QueryWindow(lng1, lat1, lng2, lat2)
    val ranges = new java.util.ArrayList[IndexRange](100)
    val quadCodeRanges = new java.util.ArrayList[IndexRange](256)
    val qOrderRanges = new java.util.ArrayList[IndexRange](256)
    val remaining = new java.util.ArrayDeque[QOrderTreeNode](math.max(16, qOrderRoots.size()))

    qOrderRoots.asScala.foreach(remaining.add)

    var containedQuadCount = 0
    var intersectQuadCount = 0

    val pp = jedis.pipelined()
    val pendingIntersects = new java.util.ArrayList[PendingIntersect]()

    def enqueueIntersectNode(node: QOrderTreeNode): Unit = {
      val signature = calculateSignature(queryWindow, node)
      val unionMask = unionMasks.get(lang.Integer.valueOf(node.qOrder))

      qOrderRanges.add(IndexRange(node.qOrder.toLong, node.qOrder.toLong, contained = false))

      if (unionMask != null && (signature & unionMask.longValue()) == 0L) {
        if (debug && printedIntersect < debugLimit) {
          println(
            s"[LETI native dbg] skip intersect by union-mask: qOrder=${node.qOrder}, quadCode=${node.quadCode}, signature=$signature, unionMask=${unionMask.longValue()}"
          )
          printedIntersect += 1
        }
        return
      }

      pendingIntersects.add(PendingIntersect(node.qOrder, signature))

      if (debug && printedIntersect < debugLimit) {
        println(
          s"[LETI native dbg] enqueue intersect: qOrder=${node.qOrder}, quadCode=${node.quadCode}, level=${node.level}, signature=$signature"
        )
        printedIntersect += 1
      }
    }

    def processPipelineResults(): Unit = {
      val intersectBatches = buildIntersectBatches(pendingIntersects, indexTable)
      val batchSetupIt = intersectBatches.iterator()
      while (batchSetupIt.hasNext) {
        val batch = batchSetupIt.next()
        val cachedTuples = LETILocSIndex.getCachedRedisRange(batch.cacheKey)
        if (cachedTuples != null) {
          batch.cachedTuples = cachedTuples
        } else {
          batch.response = pp.zrangeByScoreWithScores(indexTable, batch.minScore, batch.maxScore)
          redisAccessCount += 1L
        }
      }

      pp.sync()

      val taskIt = intersectBatches.iterator()
      while (taskIt.hasNext) {
        val task = taskIt.next()
        val result =
          if (task.cachedTuples != null) {
            task.cachedTuples
          } else {
            val loaded = task.response.get()
            if (loaded != null) {
              val loadedList = new java.util.ArrayList[redis.clients.jedis.resps.Tuple](loaded)
              LETILocSIndex.putCachedRedisRange(task.cacheKey, loadedList)
              loadedList
            } else {
              null
            }
          }
        if (result != null) {
          if (debug && printedResult < debugLimit) {
            println(
              s"[LETI native dbg] process intersect batch: qOrder=[${task.startQOrder},${task.endQOrder}], redisTupleCount=${result.size()}"
            )
            printedResult += 1
          }
          val it = result.iterator()
          val passedByQOrder = new java.util.HashMap[lang.Integer, lang.Long]()
          while (it.hasNext) {
            val elem = it.next()
            val payload = elem.getBinaryElement
            val originIndex = Bytes.toLong(payload, 8)
            val tupleQOrder = (originIndex >>> redisShapeMoveBits).toInt
            val shapeCode = originIndex & lowBitsMask(redisShapeMoveBits)
            val signature = task.signatureByQOrder.get(lang.Integer.valueOf(tupleQOrder))

            redisTupleTotal += 1
            if (signature != null && (signature.longValue() & shapeCode) != 0L) {
              val shapeOrder = Bytes.toInt(payload, 4)
              val rowKey = (tupleQOrder.toLong << mainTableMoveBits) | shapeOrder
              ranges.add(IndexRange(rowKey, rowKey, contained = false))
              redisTuplePassed += 1
              val key = lang.Integer.valueOf(tupleQOrder)
              val previous = passedByQOrder.get(key)
              passedByQOrder.put(key, lang.Long.valueOf(if (previous == null) 1L else previous.longValue() + 1L))
            }
          }

          if (debug && !passedByQOrder.isEmpty && printedResult < debugLimit) {
            println(
              s"[LETI native dbg] passed in batch: qOrder=[${task.startQOrder},${task.endQOrder}], passedQOrders=${passedByQOrder.size()}"
            )
            printedResult += 1
          }
        }
      }
    }

    while (!remaining.isEmpty) {
      val node = remaining.poll()
      if (isContained(queryWindow, node)) {
        containedQuadCount += 1
        quadCodeRanges.add(IndexRange(node.quadCode, node.subtreeUpper, contained = true))
        ranges.addAll(node.containedIntervals.rowKeyRanges)
        qOrderRanges.addAll(node.containedIntervals.qOrderRanges)
      } else if (insertion(queryWindow, node)) {
        intersectQuadCount += 1
        quadCodeRanges.add(IndexRange(node.quadCode, node.quadCode, contained = false))
        enqueueIntersectNode(node)
        node.children.asScala.foreach(remaining.add)
      }
    }

    processPipelineResults()
    pp.close()
    jedis.close()

    val merged = mergeRanges(ranges)
    val redisShapeFilterRate =
      if (redisTupleTotal > 0L) (redisTuplePassed * 100L) / redisTupleTotal else 0L

    LETILocSIndex.setLastRangeStats(
      LETILocSIndex.RangeStats(
        containedQuadCount.toLong,
        intersectQuadCount.toLong,
        redisAccessCount,
        redisShapeFilterRate
      )
    )
    RangeStatsBridge.setLast(
      RangeStatsBridge.Kind.LETI,
      containedQuadCount.toLong,
      intersectQuadCount.toLong,
      mergeRanges(quadCodeRanges).size().toLong,
      mergeRanges(qOrderRanges).size().toLong,
      redisAccessCount,
      redisShapeFilterRate
    )
    RangeDebugBridge.setLastLETI(mergeRanges(quadCodeRanges), mergeRanges(qOrderRanges))
    merged
  }

  private def buildIntersectBatches(
                                     pendingIntersects: java.util.List[PendingIntersect],
                                     indexTable: String
                                   ): java.util.ArrayList[IntersectBatch] = {
    val batches = new java.util.ArrayList[IntersectBatch]()
    if (pendingIntersects == null || pendingIntersects.isEmpty) {
      return batches
    }

    val sorted = pendingIntersects.asScala.sortBy(_.qOrder)
    var currentStart = Int.MinValue
    var currentEnd = Int.MinValue
    var currentMap: java.util.HashMap[lang.Integer, lang.Long] = null

    def appendBatch(): Unit = {
      if (currentMap != null && !currentMap.isEmpty) {
        val minScore = currentStart.toLong << redisShapeMoveBits
        val maxScore = ((currentEnd.toLong + 1L) << redisShapeMoveBits) - 1L
        batches.add(
          IntersectBatch(
            currentStart,
            currentEnd,
            currentMap,
            LETILocSIndex.rangeCacheKey(indexTable, minScore, maxScore),
            minScore,
            maxScore
          )
        )
      }
    }

    sorted.foreach { pending =>
      val qOrder = pending.qOrder
      if (currentMap == null) {
        currentStart = qOrder
        currentEnd = qOrder
        currentMap = new java.util.HashMap[lang.Integer, lang.Long]()
      } else if (qOrder > currentEnd + 1) {
        appendBatch()
        currentStart = qOrder
        currentEnd = qOrder
        currentMap = new java.util.HashMap[lang.Integer, lang.Long]()
      } else if (qOrder > currentEnd) {
        currentEnd = qOrder
      }

      val key = lang.Integer.valueOf(qOrder)
      val previous = currentMap.get(key)
      require(
        previous == null,
        s"Duplicate qOrder detected while building intersect batches: qOrder=$qOrder"
      )
      currentMap.put(key, lang.Long.valueOf(pending.signature))
    }

    appendBatch()
    batches
  }

  private def getContainedIntervalsFromCoverageHint(
                                                     quadCode: Long
                                                   ): LETILocSIndex.ContainedIntervals = {
    val codeKey = lang.Long.valueOf(quadCode)
    val order = quadCodeOrder.get(codeKey)
    val parent = quadCodeParentQuad.get(codeKey)
    if (order == null || parent == null || parent.elementCode != quadCode) {
      return null
    }

    if (supportsContiguousSubtreeOrders && parent.validChildCount >= 0) {
      return buildContainedIntervalsFromRange(order.intValue(), order.intValue() + parent.validChildCount)
    }

    if (parent.effectiveSubtreeOrders != null && parent.effectiveSubtreeOrders.length > 0) {
      return buildContainedIntervalsFromOrderList(order.intValue(), parent.effectiveSubtreeOrders)
    }

    buildContainedIntervalsFromRange(order.intValue(), order.intValue())
  }

  private def buildContainedIntervalsFromRange(
                                                startQOrder: Int,
                                                endQOrder: Int
                                              ): LETILocSIndex.ContainedIntervals = {
    val rowKeyRanges = new java.util.ArrayList[IndexRange]()
    val qOrderRanges = new java.util.ArrayList[IndexRange]()
    if (endQOrder < startQOrder) {
      return LETILocSIndex.ContainedIntervals(rowKeyRanges, qOrderRanges)
    }

    appendContainedRunIntervals(startQOrder, endQOrder, qOrderRanges, rowKeyRanges)

    LETILocSIndex.ContainedIntervals(rowKeyRanges, qOrderRanges)
  }

  private def buildContainedIntervalsFromOrderList(
                                                    selfQOrder: Int,
                                                    descendantOrders: Array[Int]
                                                  ): LETILocSIndex.ContainedIntervals = {
    val rowKeyRanges = new java.util.ArrayList[IndexRange]()
    val qOrderRanges = new java.util.ArrayList[IndexRange]()

    val size = if (descendantOrders == null) 1 else descendantOrders.length + 1
    val allOrders = new Array[Int](size)
    allOrders(0) = selfQOrder
    if (descendantOrders != null && descendantOrders.length > 0) {
      System.arraycopy(descendantOrders, 0, allOrders, 1, descendantOrders.length)
    }
    java.util.Arrays.sort(allOrders)

    var runStart = allOrders(0)
    var previous = allOrders(0)
    var idx = 1
    while (idx < allOrders.length) {
      val current = allOrders(idx)
      if (current != previous + 1) {
        appendContainedRunIntervals(runStart, previous, qOrderRanges, rowKeyRanges)
        runStart = current
      }
      previous = current
      idx += 1
    }

    appendContainedRunIntervals(runStart, previous, qOrderRanges, rowKeyRanges)

    LETILocSIndex.ContainedIntervals(rowKeyRanges, qOrderRanges)
  }

  private def appendContainedRunIntervals(
                                           runStart: Int,
                                           runEnd: Int,
                                           qOrderRanges: java.util.List[IndexRange],
                                           rowKeyRanges: java.util.List[IndexRange]
                                         ): Unit = {
    qOrderRanges.add(IndexRange(runStart.toLong, runEnd.toLong, contained = true))
    rowKeyRanges.add(IndexRange(runStart.toLong << mainTableMoveBits, ((runEnd.toLong + 1L) << mainTableMoveBits) - 1L, contained = true))
  }

  /**
   * 计算查询窗口在父 quad 的扩展单元格（EE）中的签名
   *
   * 计算逻辑：
   * 1. parentQuad 的边界定义基础单元格（cell）
   * 2. EE 边界 = cell 向右上扩展（全局 alpha * cellWidth, 全局 beta * cellHeight）
   * 3. 在 EE 内划分网格：
   *    - 全局划分：使用全局 alpha 和 beta
   *    - 自适应划分：使用 parentQuad.alpha 和 parentQuad.beta
   *      4. 返回查询窗口覆盖的网格单元位掩码
   *
   * @param qw         查询窗口
   * @param parentQuad 父 quad 信息（包含基础单元格边界和自适应参数）
   * @return 签名位掩码，每一位表示一个网格单元是否被查询窗口覆盖
   */
  private def buildQOrderTree(): java.util.List[QOrderTreeNode] = {
    val roots = new java.util.ArrayList[QOrderTreeNode]()
    if (validQuadCodes == null || validQuadCodes.isEmpty) {
      return roots
    }

    val nodes = validQuadCodes.asScala.toSeq
      .flatMap { codeBox =>
        val code = codeBox.longValue()
        val order = quadCodeOrder.get(codeBox)
        val parent = quadCodeParentQuad.get(codeBox)
        // Build the native tree from effective LETI nodes only:
        // quad_code[0] / parent.elementCode is the effective node, while the
        // remaining quad_code entries are absorbed raw descendants sharing the same order.
        if (order == null || parent == null) {
          None
        } else {
          val cellWidth = parent.xmax - parent.xmin
          val cellHeight = parent.ymax - parent.ymin
          val eeXmin = parent.xmin
          val eeYmin = parent.ymin
          val eeXmax = parent.xmin + alpha * cellWidth
          val eeYmax = parent.ymin + beta * cellHeight
          val partitionAlpha = if (adaptivePartition) parent.alpha else alpha
          val partitionBeta = if (adaptivePartition) parent.beta else beta
          val minScore = order.longValue() << redisShapeMoveBits
          val maxScore = ((order.longValue() + 1L) << redisShapeMoveBits) - 1L
          Some(
            QOrderTreeNode(
              quadCode = code,
              qOrder = order.intValue(),
              parentQuad = parent,
              level = parent.level,
              subtreeUpper = code + IS(parent.level)
              ,
              eeXmin = eeXmin,
              eeYmin = eeYmin,
              eeXmax = eeXmax,
              eeYmax = eeYmax,
              partitionAlpha = partitionAlpha,
              partitionBeta = partitionBeta,
              minScore = minScore,
              maxScore = maxScore
            )
          )
        }
      }
      .sortBy(_.quadCode)

    val stack = new java.util.ArrayDeque[QOrderTreeNode]()
    nodes.foreach { node =>
      // A valid LETI child must be fully contained in its parent's subtree interval.
      // Checking only node.quadCode <= parent.subtreeUpper is not enough here,
      // because a nested valid node may have a subtree that extends beyond that parent.
      while (!stack.isEmpty &&
        !(node.quadCode >= stack.peekLast().quadCode && node.subtreeUpper <= stack.peekLast().subtreeUpper)) {
        stack.removeLast()
      }
      if (stack.isEmpty) {
        roots.add(node)
      } else {
        stack.peekLast().children.add(node)
      }
      stack.addLast(node)
    }

    roots.asScala.foreach(assignContainedIntervalsFromCoverageHint)
    roots
  }

  private def assignContainedIntervalsFromCoverageHint(node: QOrderTreeNode): Unit = {
    node.containedIntervals = getContainedIntervalsFromCoverageHint(node.quadCode)
    node.children.asScala.foreach(assignContainedIntervalsFromCoverageHint)
  }

  private def insertion(window: QueryWindow, node: QOrderTreeNode): Boolean = {
    window.xmax >= node.eeXmin &&
      window.ymax >= node.eeYmin &&
      window.xmin <= node.eeXmax &&
      window.ymin <= node.eeYmax
  }

  private def isContained(window: QueryWindow, node: QOrderTreeNode): Boolean = {
    window.xmin <= node.eeXmin &&
      window.ymin <= node.eeYmin &&
      window.xmax >= node.eeXmax &&
      window.ymax >= node.eeYmax
  }

  private def calculateSignature(qw: QueryWindow, node: QOrderTreeNode): Long = {
    var signature = 0L

    val gridCellW = (node.eeXmax - node.eeXmin) / node.partitionAlpha
    val gridCellH = (node.eeYmax - node.eeYmin) / node.partitionBeta

    val xs = math.max(0, math.floor((qw.xmin - node.eeXmin) / gridCellW).toInt)
    val xe = math.min(node.partitionAlpha, math.floor((qw.xmax - node.eeXmin) / gridCellW).toInt + 1)

    val ys = math.max(0, math.floor((qw.ymin - node.eeYmin) / gridCellH).toInt)
    val ye = math.min(node.partitionBeta, math.floor((qw.ymax - node.eeYmin) / gridCellH).toInt + 1)

    var i = xs
    while (i < xe) {
      var j = ys
      while (j < ye) {
        val minX = node.eeXmin + gridCellW * i
        val minY = node.eeYmin + gridCellH * j
        val maxX = minX + gridCellW
        val maxY = minY + gridCellH

        if (qw.insertion(minX, minY, maxX, maxY)) {
          signature |= (1L << (i * node.partitionBeta + j))
        }
        j += 1
      }
      i += 1
    }
    signature
  }

  private def calculateSignature(qw: QueryWindow, parentQuad: ParentQuad): Long = {
    var signature = 0L

    val cellXmin = parentQuad.xmin
    val cellYmin = parentQuad.ymin
    val cellWidth = parentQuad.xmax - cellXmin
    val cellHeight = parentQuad.ymax - cellYmin

    val eeXmin = cellXmin
    val eeYmin = cellYmin
    val eeXmax = cellXmin + alpha * cellWidth
    val eeYmax = cellYmin + beta * cellHeight

    val qXmin = qw.xmin
    val qYmin = qw.ymin
    val qXmax = qw.xmax
    val qYmax = qw.ymax

    val partitionAlpha = if (adaptivePartition) parentQuad.alpha else alpha
    val partitionBeta = if (adaptivePartition) parentQuad.beta else beta

    val gridCellW = (eeXmax - eeXmin) / partitionAlpha
    val gridCellH = (eeYmax - eeYmin) / partitionBeta

    val xs = math.max(0, math.floor((qXmin - eeXmin) / gridCellW).toInt)
    val xe = math.min(partitionAlpha, math.floor((qXmax - eeXmin) / gridCellW).toInt + 1)

    val ys = math.max(0, math.floor((qYmin - eeYmin) / gridCellH).toInt)
    val ye = math.min(partitionBeta, math.floor((qYmax - eeYmin) / gridCellH).toInt + 1)

    var i = xs
    while (i < xe) {
      var j = ys
      while (j < ye) {
        val minX = eeXmin + gridCellW * i
        val minY = eeYmin + gridCellH * j
        val maxX = minX + gridCellW
        val maxY = minY + gridCellH

        if (qw.insertion(minX, minY, maxX, maxY)) {
          signature |= (1L << (i * partitionBeta + j))
        }
        j += 1
      }
      i += 1
    }
    signature
  }

  /**
   * 分裂当前 quad 并加入子节点到待处理队列
   *
   * @param quad      当前 quad
   * @param remaining 待处理的 quad 队列
   */
  /**
   * 将完全包含的 quadOrder 列表合并为连续的索引范围
   *
   * @param childrenOrders 完全包含的 quadOrder 列表
   * @return 合并后的索引范围列表
   */
  /**
   * 合并重叠或相邻的索引范围
   *
   * @param ranges 待合并的索引范围列表
   * @return 合并后的索引范围列表
   */
  private def mergeRanges(ranges: java.util.ArrayList[IndexRange]): java.util.List[IndexRange] = {

    if (ranges.size() <= 1) return ranges

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
    result.asJava
  }

  private def lowBitsMask(bits: Int): Long = {
    if (bits <= 0) 0L
    else if (bits >= 63) -1L
    else (1L << bits) - 1L
  }
}

object LETILocSIndex extends Serializable {
  case class ContainedIntervals(
                                 rowKeyRanges: java.util.List[IndexRange],
                                 qOrderRanges: java.util.List[IndexRange]
                               ) extends Serializable

  private val maxRedisRangeCacheSize: Int =
    java.lang.Integer.getInteger("tman.leti.redisRangeCacheSize", 200000)

  private val redisRangeCache =
    new java.util.LinkedHashMap[String, java.util.List[redis.clients.jedis.resps.Tuple]](16, 0.75f, true) {
      override def removeEldestEntry(
                                      eldest: java.util.Map.Entry[String, java.util.List[redis.clients.jedis.resps.Tuple]]
                                    ): Boolean = {
        this.size() > maxRedisRangeCacheSize
      }
    }

  private def withCacheLock[T](f: => T): T = redisRangeCache.synchronized {
    f
  }

  private val unionMaskCache =
    new java.util.concurrent.ConcurrentHashMap[String, java.util.Map[lang.Integer, lang.Long]]()

  private def rangeCacheKey(indexTable: String, minScore: Long, maxScore: Long): String =
    indexTable + ":" + minScore + ":" + maxScore

  private def unionMaskKey(indexTable: String): String =
    indexTable + ":leti:union_mask"

  private def getCachedRedisRange(
                                   key: String
                                 ): java.util.List[redis.clients.jedis.resps.Tuple] = withCacheLock {
    redisRangeCache.get(key)
  }

  private def putCachedRedisRange(
                                   key: String,
                                   tuples: java.util.List[redis.clients.jedis.resps.Tuple]
                                 ): Unit = withCacheLock {
    redisRangeCache.put(key, tuples)
  }

  def getCachedUnionMasks(
                           jedis: Jedis,
                           indexTable: String
                         ): java.util.Map[lang.Integer, lang.Long] = {
    val cached = unionMaskCache.get(indexTable)
    if (cached != null) return cached

    val loaded = new java.util.HashMap[lang.Integer, lang.Long]()
    val raw = jedis.hgetAll(unionMaskKey(indexTable))


    if (raw != null) {
      val it = raw.entrySet().iterator()
      while (it.hasNext) {
        val entry = it.next()
        loaded.put(lang.Integer.valueOf(entry.getKey), lang.Long.valueOf(entry.getValue))
      }
    }

    val previous = unionMaskCache.putIfAbsent(indexTable, java.util.Collections.unmodifiableMap(loaded))
    if (previous != null) previous else unionMaskCache.get(indexTable)
  }

  /**
   * Java 侧输出所需的“真实代价口径”
   * - visitedCells: containQuadCount + intersectQuadCount
   * - redisAccessCount: pipeline 内 zrangeByScoreWithScores 调用次数
   */
  case class RangeStats(
                         containedQuadCount: Long,
                         intersectQuadCount: Long,
                         redisAccessCount: Long,
                         redisShapeFilterRateScaled: Long
                       ) extends Serializable

  private val lastStatsThreadLocal = new ThreadLocal[RangeStats]()

  def setLastRangeStats(stats: RangeStats): Unit = {
    lastStatsThreadLocal.set(stats)
  }

  def getLastVisitedCellsByContainIntersect: Long = {
    val s = lastStatsThreadLocal.get()
    if (s == null) 0L else s.containedQuadCount + s.intersectQuadCount
  }

  def getLastRedisAccessCount: Long = {
    val s = lastStatsThreadLocal.get()
    if (s == null) 0L else s.redisAccessCount
  }

  def getLastRedisShapeFilterRateScaled: Long = {
    val s = lastStatsThreadLocal.get()
    if (s == null) 0L else s.redisShapeFilterRateScaled
  }

  def clearLastRangeStats(): Unit = {
    lastStatsThreadLocal.remove()
  }

  def clearProcessCaches(): Unit = {
    clearLastRangeStats()
    withCacheLock {
      redisRangeCache.clear()
    }
    unionMaskCache.clear()
    cache.clear()
    cacheBounds.clear()
  }

  /**
   * 缓存 LETILocSIndex 实例（使用默认边界：-180~180, -90~90）
   */
  private val cache =
    new java.util.concurrent.ConcurrentHashMap[(String, Short, Int, Int, Boolean, String), LETILocSIndex]()

  /**
   * 缓存 LETILocSIndex 实例（使用自定义边界）
   */
  private val cacheBounds = new java.util.concurrent.ConcurrentHashMap[
    (String, Short, Int, Int, (Double, Double), (Double, Double), Boolean, String),
    LETILocSIndex
  ]()

  def apply(g: Short, alpha: Int, beta: Int): LETILocSIndex = {
    apply(g, alpha, beta, adaptivePartition = false)
  }

  def apply(g: Short, alpha: Int, beta: Int, adaptivePartition: Boolean): LETILocSIndex = {
    apply(g, alpha, beta, adaptivePartition, LetiOrderResolver.DEFAULT_CANONICAL_PATH)
  }

  def apply(g: Short, alpha: Int, beta: Int, adaptivePartition: Boolean, orderDefinitionPath: String): LETILocSIndex = {
    val path = LetiOrderResolver.resolveClasspathResource(orderDefinitionPath)
    var sfc = cache.get(("", g, alpha, beta, adaptivePartition, path))
    if (sfc == null) {
      sfc = new LETILocSIndex(g, (-180.0, 180.0), (-90.0, 90.0), alpha, beta, adaptivePartition, path)
      cache.put(("", g, alpha, beta, adaptivePartition, path), sfc)
    }
    sfc
  }

  def applyCoverageAware(g: Short,
                         alpha: Int,
                         beta: Int,
                         adaptivePartition: Boolean,
                         orderDefinitionPath: String): LETILocSIndex = {
    apply(g, alpha, beta, adaptivePartition, orderDefinitionPath)
  }

  def apply(
             g: Short,
             xBounds: (Double, Double),
             yBounds: (Double, Double),
             alpha: Int,
             beta: Int
           ): LETILocSIndex = {
    apply(g, xBounds, yBounds, alpha, beta, adaptivePartition = false)
  }

  def apply(
             g: Short,
             xBounds: (Double, Double),
             yBounds: (Double, Double),
             alpha: Int,
             beta: Int,
             adaptivePartition: Boolean
           ): LETILocSIndex = {
    apply(g, xBounds, yBounds, alpha, beta, adaptivePartition, LetiOrderResolver.DEFAULT_CANONICAL_PATH)
  }

  def apply(
             g: Short,
             xBounds: (Double, Double),
             yBounds: (Double, Double),
             alpha: Int,
             beta: Int,
             adaptivePartition: Boolean,
             orderDefinitionPath: String
           ): LETILocSIndex = {
    val path = LetiOrderResolver.resolveClasspathResource(orderDefinitionPath)
    var sfc = cacheBounds.get(("", g, alpha, beta, xBounds, yBounds, adaptivePartition, path))
    if (sfc == null) {
      sfc = new LETILocSIndex(g, xBounds, yBounds, alpha, beta, adaptivePartition, path)
      cacheBounds.put(("", g, alpha, beta, xBounds, yBounds, adaptivePartition, path), sfc)
    }
    sfc
  }

  def applyCoverageAware(
                          g: Short,
                          xBounds: (Double, Double),
                          yBounds: (Double, Double),
                          alpha: Int,
                          beta: Int,
                          adaptivePartition: Boolean,
                          orderDefinitionPath: String
                        ): LETILocSIndex = {
    apply(g, xBounds, yBounds, alpha, beta, adaptivePartition, orderDefinitionPath)
  }

  def apply(table: String, g: Short, alpha: Int, beta: Int): LETILocSIndex = {
    apply(table, g, alpha, beta, adaptivePartition = false)
  }

  def apply(table: String, g: Short, alpha: Int, beta: Int, adaptivePartition: Boolean): LETILocSIndex = {
    apply(table, g, alpha, beta, adaptivePartition, LetiOrderResolver.DEFAULT_CANONICAL_PATH)
  }

  def apply(table: String, g: Short, alpha: Int, beta: Int, adaptivePartition: Boolean, orderDefinitionPath: String): LETILocSIndex = {
    val path = LetiOrderResolver.resolveClasspathResource(orderDefinitionPath)
    var sfc = cache.get((table, g, alpha, beta, adaptivePartition, path))
    if (sfc == null) {
      sfc = new LETILocSIndex(g, (-180.0, 180.0), (-90.0, 90.0), alpha, beta, adaptivePartition, path)
      cache.put((table, g, alpha, beta, adaptivePartition, path), sfc)
    }
    sfc
  }

  def apply(
             table: String,
             g: Short,
             xBounds: (Double, Double),
             yBounds: (Double, Double),
             alpha: Int,
             beta: Int
           ): LETILocSIndex = {
    apply(table, g, xBounds, yBounds, alpha, beta, adaptivePartition = false)
  }

  def apply(
             table: String,
             g: Short,
             xBounds: (Double, Double),
             yBounds: (Double, Double),
             alpha: Int,
             beta: Int,
             adaptivePartition: Boolean
           ): LETILocSIndex = {
    apply(table, g, xBounds, yBounds, alpha, beta, adaptivePartition, LetiOrderResolver.DEFAULT_CANONICAL_PATH)
  }

  def apply(
             table: String,
             g: Short,
             xBounds: (Double, Double),
             yBounds: (Double, Double),
             alpha: Int,
             beta: Int,
             adaptivePartition: Boolean,
             orderDefinitionPath: String
           ): LETILocSIndex = {
    val path = LetiOrderResolver.resolveClasspathResource(orderDefinitionPath)
    var sfc = cacheBounds.get((table, g, alpha, beta, xBounds, yBounds, adaptivePartition, path))
    if (sfc == null) {
      sfc = new LETILocSIndex(g, xBounds, yBounds, alpha, beta, adaptivePartition, path)
      cacheBounds.put((table, g, alpha, beta, xBounds, yBounds, adaptivePartition, path), sfc)
    }
    sfc
  }
}
