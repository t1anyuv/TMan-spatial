package index

import org.apache.hadoop.hbase.util.Bytes
import org.locationtech.sfcurve.IndexRange
import redis.clients.jedis.Jedis
import utils.LetiOrderResolver
import utils.LSFCReader
import utils.LSFCReader.{ParentQuad, loadFromClasspath}

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
    loadFromClasspath(LetiOrderResolver.resolveClasspathResource(orderDefinitionPath))

  private val quadCodeOrder: util.Map[lang.Long, Integer] = mapper.quadCodeOrder
  private val quadCodeParentQuad: util.Map[lang.Long, ParentQuad] = mapper.quadCodeParentQuad
  private val validQuadCodes: util.Set[lang.Long] = mapper.validQuadCodes

  private val sortedQuadCodes: java.util.NavigableSet[lang.Long] = mapper.sortedQuadCodes
  private val containedIntervalCache =
    new java.util.concurrent.ConcurrentHashMap[lang.Long, LETILocSIndex.ContainedIntervals]()

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

  /**
   * 最大形状位数，用于统一编码空间
   * 自适应划分时从 order metadata 获取，否则使用全局 alpha * beta
   */
  private val metaMaxShapeBits: Int = if (mapper.metadata != null) mapper.metadata.maxShapeBits else alpha * beta

  private val orderCountHint: Int =
    if (mapper.metadata != null && mapper.metadata.orderCount > 0) mapper.metadata.orderCount
    else quadCodeOrder.size()

  /**
   * 形状编码的位数
   * 自适应划分时使用 maxShapeBits，否则使用全局 alpha * beta
   */
  private val moveBits: Int = if (adaptivePartition && metaMaxShapeBits > 0) metaMaxShapeBits else alpha * beta

  /**
   * Contained qOrder 连续段的最大合并长度。
   * - <= 0: 不限制，保持完全合并
   * - > 0 : 超过该长度时按块拆分，避免形成过宽 rowkey 区间
   */
  private val maxContainedOrderRunLength: Int =
    java.lang.Integer.getInteger("tman.leti.maxContainedOrderRunLength", 32)

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
    val remaining = new java.util.ArrayDeque[TShapeEE](200)

    /** 查询窗口完全覆盖的 quad 个数（覆盖分支） */
    var containedQuadCount = 0
    /** 与查询窗口相交但非完全包含的 quad 个数（相交分支） */
    var intersectQuadCount = 0

    val pp = jedis.pipelined()
    case class IntersectTask(
        qOrder: Int,
        signature: Long,
        cacheKey: String,
        cachedTuples: java.util.List[redis.clients.jedis.resps.Tuple],
        response: redis.clients.jedis.Response[_ <: java.util.Collection[
          redis.clients.jedis.resps.Tuple
        ]]
    )
    val intersectTasks = new java.util.ArrayList[IntersectTask]()

    addChildren(root, remaining)

    def processIntersectQuad(quad: TShapeEE): Unit = {
      val quadCode = quad.elementCode
      val quadOrder = quadCodeOrder.get(quadCode)
      val parentQuad = quadCodeParentQuad.get(quadCode)

      assert(quadOrder != null, s"Quad order should not be null for quadCode: $quadCode")
      assert(parentQuad != null, s"Parent quad should not be null for quadCode: $quadCode")

      val signature = calculateSignature(queryWindow, parentQuad)
      val unionMask = unionMasks.get(quadOrder)

      qOrderRanges.add(IndexRange(quadOrder.toLong, quadOrder.toLong, contained = false))

      if (unionMask != null && (signature & unionMask.longValue()) == 0L) {
        if (debug && printedIntersect < debugLimit) {
          println(
            s"[LETI dbg] skip intersect by union-mask: qOrder=${quadOrder.toInt}, quadCode=$quadCode, signature=$signature, unionMask=${unionMask.longValue()}"
          )
          printedIntersect += 1
        }
        return
      }

      val minScore = quadOrder.toLong << moveBits
      val maxScore = ((quadOrder + 1L) << moveBits) - 1L

      val cacheKey = LETILocSIndex.rangeCacheKey(indexTable, minScore, maxScore)
      val cachedTuples = LETILocSIndex.getCachedRedisRange(cacheKey)

      if (cachedTuples != null) {
        intersectTasks.add(IntersectTask(quadOrder.toInt, signature, cacheKey, cachedTuples, null))
      } else {
        val response = pp.zrangeByScoreWithScores(indexTable, minScore, maxScore)
        redisAccessCount += 1L
        intersectTasks.add(IntersectTask(quadOrder.toInt, signature, cacheKey, null, response))
      }

      if (debug && printedIntersect < debugLimit) {
        println(
          s"[LETI dbg] enqueue intersect: qOrder=${quadOrder.toInt}, quadCode=$quadCode, level=${quad.level}, signature=$signature"
        )
        printedIntersect += 1
      }
    }

    /**
     * 处理 Redis Pipeline 返回的结果
     * Redis ZSET 的 score = originIndex = (quadOrder << moveBits) | shapeCode
     * Redis ZSET 的 value = payload，格式：[0-4)=count, [4-8)=shapeOrder, [8-16)=originIndex
     */
    def processPipelineResults(): Unit = {
      val taskIt = intersectTasks.iterator()
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
              s"[LETI dbg] process intersect: qOrder=${task.qOrder}, signature=${task.signature}, redisTupleCount=${result.size()}"
            )
            printedResult += 1
          }
          val it = result.iterator()
          var passedInTask = 0L
          while (it.hasNext) {
            val elem = it.next()
            val payload = elem.getBinaryElement

            val originIndex = Bytes.toLong(payload, 8)
            val shapeCode = originIndex & ((1L << moveBits) - 1)

            // 签名匹配：signature=0 表示精度问题，直接放行；否则检查 shapeCode 是否与查询签名相交
            // if (task.signature == 0 || (task.signature & shapeCode) != 0)
            redisTupleTotal += 1
            if ((task.signature & shapeCode) != 0) {
              val shapeOrder = Bytes.toInt(payload, 4)
              val rowKey = (task.qOrder.toLong << moveBits) | shapeOrder
              ranges.add(IndexRange(rowKey, rowKey, contained = false))
              redisTuplePassed += 1
              passedInTask += 1
            }
          }

          if (debug && passedInTask > 0 && printedResult < debugLimit) {
            println(s"[LETI dbg] passed in task: qOrder=${task.qOrder}, passedTuples=$passedInTask")
            printedResult += 1
          }
        }
      }
    }
    while (!remaining.isEmpty) {
      val quad = remaining.poll()
      val quadCode = quad.elementCode
      if (validQuadCodes.contains(quadCode)) {
        if (quad.isContained(queryWindow)) {
          containedQuadCount += 1
          val qMin = quad.elementCode
          val qMax = quad.elementCode + IS(quad.level.toShort)
          quadCodeRanges.add(IndexRange(qMin, qMax, contained = true))
          val containedIntervals = getContainedIntervals(quad)
          ranges.addAll(containedIntervals.rowKeyRanges)
          qOrderRanges.addAll(containedIntervals.qOrderRanges)
        } else if (quad.insertion(queryWindow)) {
          intersectQuadCount += 1
          quadCodeRanges.add(IndexRange(quad.elementCode, quad.elementCode, contained = false))
          processIntersectQuad(quad)
          if (quad.level < maxR) {
            addChildren(quad, remaining)
          }
        }
      } else if (quad.level < maxR) {
        addChildren(quad, remaining)
      }
    }

    pp.sync()
    processPipelineResults()

    pp.close()
    jedis.close()

    if (debug) {
      println(
        s"[LETILocSIndex.ranges] 覆盖 quad 数: $containedQuadCount, 相交 quad 数: $intersectQuadCount"
      )
    }

    val merged = mergeRanges(ranges)
    if (debug) {
      println(
        s"[LETI dbg] rangesBeforeMerge=${ranges.size()}, rangesAfterMerge=${merged.size()}, redisTupleTotal=$redisTupleTotal, redisTuplePassed=$redisTuplePassed"
      )
    }

    // update stats
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
      redisAccessCount,
      redisShapeFilterRate
    )
    RangeDebugBridge.setLastLETI(mergeRanges(quadCodeRanges), mergeRanges(qOrderRanges))
    merged
  }

  /**
   * 与 TShape 默认 ranges 流程对齐的 LETI 版本。
   */
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
    case class IntersectTask(
                              node: QOrderTreeNode,
                              signature: Long,
                              cacheKey: String,
                              cachedTuples: java.util.List[redis.clients.jedis.resps.Tuple],
                              response: redis.clients.jedis.Response[_ <: java.util.Collection[
                                redis.clients.jedis.resps.Tuple
                              ]]
                            )
    val intersectTasks = new java.util.ArrayList[IntersectTask]()

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

      val cacheKey = LETILocSIndex.rangeCacheKey(indexTable, node.minScore, node.maxScore)
      val cachedTuples = LETILocSIndex.getCachedRedisRange(cacheKey)

      if (cachedTuples != null) {
        intersectTasks.add(IntersectTask(node, signature, cacheKey, cachedTuples, null))
      } else {
        val response = pp.zrangeByScoreWithScores(indexTable, node.minScore, node.maxScore)
        redisAccessCount += 1L
        intersectTasks.add(IntersectTask(node, signature, cacheKey, null, response))
      }

      if (debug && printedIntersect < debugLimit) {
        println(
          s"[LETI native dbg] enqueue intersect: qOrder=${node.qOrder}, quadCode=${node.quadCode}, level=${node.level}, signature=$signature"
        )
        printedIntersect += 1
      }
    }

    def processPipelineResults(): Unit = {
      val taskIt = intersectTasks.iterator()
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
              s"[LETI native dbg] process intersect: qOrder=${task.node.qOrder}, signature=${task.signature}, redisTupleCount=${result.size()}"
            )
            printedResult += 1
          }
          val it = result.iterator()
          var passedInTask = 0L
          while (it.hasNext) {
            val elem = it.next()
            val payload = elem.getBinaryElement
            val originIndex = Bytes.toLong(payload, 8)
            val shapeCode = originIndex & ((1L << moveBits) - 1L)

            redisTupleTotal += 1
            if ((task.signature & shapeCode) != 0L) {
              val shapeOrder = Bytes.toInt(payload, 4)
              val rowKey = (task.node.qOrder.toLong << moveBits) | shapeOrder
              ranges.add(IndexRange(rowKey, rowKey, contained = false))
              redisTuplePassed += 1
              passedInTask += 1L
            }
          }

          if (debug && passedInTask > 0 && printedResult < debugLimit) {
            println(
              s"[LETI native dbg] passed in task: qOrder=${task.node.qOrder}, passedTuples=$passedInTask"
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

    pp.sync()
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
      redisAccessCount,
      redisShapeFilterRate
    )
    RangeDebugBridge.setLastLETI(mergeRanges(quadCodeRanges), mergeRanges(qOrderRanges))
    merged
  }

  private def getContainedIntervals(quad: TShapeEE): LETILocSIndex.ContainedIntervals = {
    val key = lang.Long.valueOf(quad.elementCode)
    var cached = containedIntervalCache.get(key)
    if (cached == null) {
      cached = buildContainedIntervals(quad.elementCode, quad.level)
      val previous = containedIntervalCache.putIfAbsent(key, cached)
      if (previous != null) {
        cached = previous
      }
    }
    cached
  }

  private def getContainedIntervalsWithoutSplit(quad: TShapeEE): LETILocSIndex.ContainedIntervals = {
    val minCode = lang.Long.valueOf(quad.elementCode)
    val maxCode = lang.Long.valueOf(quad.elementCode + IS(quad.level.toShort))
    val codes = sortedQuadCodes.subSet(minCode, true, maxCode, true)
    val orderBits = new java.util.BitSet(math.max(orderCountHint, 64))
    val it = codes.iterator()
    while (it.hasNext) {
      val code = it.next()
      val order = quadCodeOrder.get(code)
      if (order != null) {
        orderBits.set(order.intValue())
      }
    }
    buildIntervalsFromBitSetWithoutSplit(orderBits)
  }

  private def buildContainedIntervals(quadCode: Long, level: Int): LETILocSIndex.ContainedIntervals = {
    val minCode = lang.Long.valueOf(quadCode)
    val maxCode = lang.Long.valueOf(quadCode + IS(level.toShort))
    val codes = sortedQuadCodes.subSet(minCode, true, maxCode, true)
    val orderBits = new java.util.BitSet(math.max(orderCountHint, 64))
    val it = codes.iterator()
    while (it.hasNext) {
      val code = it.next()
      val order = quadCodeOrder.get(code)
      if (order != null) {
        orderBits.set(order.intValue())
      }
    }
    buildIntervalsFromBitSet(orderBits)
  }

  private def buildIntervalsFromBitSet(orderBits: java.util.BitSet): LETILocSIndex.ContainedIntervals = {
    val rowKeyRanges = new java.util.ArrayList[IndexRange]()
    val qOrderRanges = new java.util.ArrayList[IndexRange]()
    if (orderBits == null || orderBits.isEmpty) {
      return LETILocSIndex.ContainedIntervals(rowKeyRanges, qOrderRanges)
    }

    var start = orderBits.nextSetBit(0)
    while (start >= 0) {
      var end = start
      var next = orderBits.nextSetBit(start + 1)
      while (next >= 0 && next == end + 1) {
        end = next
        next = orderBits.nextSetBit(end + 1)
      }

      appendContainedRunIntervals(start, end, qOrderRanges, rowKeyRanges)
      start = next
    }
    LETILocSIndex.ContainedIntervals(rowKeyRanges, qOrderRanges)
  }

  private def buildIntervalsFromBitSetWithoutSplit(orderBits: java.util.BitSet): LETILocSIndex.ContainedIntervals = {
    val rowKeyRanges = new java.util.ArrayList[IndexRange]()
    val qOrderRanges = new java.util.ArrayList[IndexRange]()
    if (orderBits == null || orderBits.isEmpty) {
      return LETILocSIndex.ContainedIntervals(rowKeyRanges, qOrderRanges)
    }

    var start = orderBits.nextSetBit(0)
    while (start >= 0) {
      var end = start
      var next = orderBits.nextSetBit(start + 1)
      while (next >= 0 && next == end + 1) {
        end = next
        next = orderBits.nextSetBit(end + 1)
      }

      qOrderRanges.add(IndexRange(start.toLong, end.toLong, contained = true))
      rowKeyRanges.add(
        IndexRange(
          start.toLong << moveBits,
          ((end.toLong + 1L) << moveBits) - 1L,
          contained = true
        )
      )
      start = next
    }
    LETILocSIndex.ContainedIntervals(rowKeyRanges, qOrderRanges)
  }

  private def appendContainedRunIntervals(
                                           runStart: Int,
                                           runEnd: Int,
                                           qOrderRanges: java.util.List[IndexRange],
                                           rowKeyRanges: java.util.List[IndexRange]
                                         ): Unit = {
    if (maxContainedOrderRunLength <= 0) {
      qOrderRanges.add(IndexRange(runStart.toLong, runEnd.toLong, contained = true))
      rowKeyRanges.add(IndexRange(runStart.toLong << moveBits, ((runEnd.toLong + 1L) << moveBits) - 1L, contained = true))
      return
    }

    var blockStart = runStart
    while (blockStart <= runEnd) {
      val blockEnd = math.min(runEnd, blockStart + maxContainedOrderRunLength - 1)
      qOrderRanges.add(IndexRange(blockStart.toLong, blockEnd.toLong, contained = true))
      rowKeyRanges.add(IndexRange(blockStart.toLong << moveBits, ((blockEnd.toLong + 1L) << moveBits) - 1L, contained = true))
      blockStart = blockEnd + 1
    }
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
          val minScore = order.longValue() << moveBits
          val maxScore = ((order.longValue() + 1L) << moveBits) - 1L
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
      while (!stack.isEmpty && node.quadCode > stack.peekLast().subtreeUpper) {
        stack.removeLast()
      }
      if (stack.isEmpty) {
        roots.add(node)
      } else {
        stack.peekLast().children.add(node)
      }
      stack.addLast(node)
    }

    roots.asScala.foreach(populateContainedIntervals)
    roots
  }

  private def populateContainedIntervals(node: QOrderTreeNode): java.util.BitSet = {
    val orderBits = new java.util.BitSet(math.max(orderCountHint, 64))
    orderBits.set(node.qOrder)
    node.children.asScala.foreach { child =>
      orderBits.or(populateContainedIntervals(child))
    }
    node.containedIntervals = buildIntervalsFromBitSet(orderBits)
    orderBits
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
  private def addChildren(quad: TShapeEE, remaining: util.ArrayDeque[TShapeEE]): Unit = {
    quad.split()
    var i = 0
    while (i < 4) {
      val child = quad.children.get(i)
      if (child != null) remaining.add(child)
      i += 1
    }
  }

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
