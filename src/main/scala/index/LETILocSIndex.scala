package index

import entity.Trajectory
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
                                     children: java.util.ArrayList[QOrderTreeNode] = new java.util.ArrayList[QOrderTreeNode](),
                                     var containedIntervals: LETILocSIndex.ContainedIntervals = LETILocSIndex.ContainedIntervals(
                                       new java.util.ArrayList[IndexRange](),
                                       new java.util.ArrayList[IndexRange]()
                                     )
                                   )

  private case class PendingIntersect(node: QOrderTreeNode, signature: Long)

  private case class IntersectLookup(
                                      node: QOrderTreeNode,
                                      signature: Long,
                                      cacheKey: String,
                                      var cachedPayload: Array[Byte] = null,
                                      var response: redis.clients.jedis.Response[Array[Byte]] = null
                                    )

  private case class DebugState(intersectLimit: Int, resultLimit: Int, var printedIntersect: Int = 0, var printedResult: Int = 0)

  private case class TraversalResult(containedQuadCount: Int, intersectQuadCount: Int, pendingIntersects: java.util.ArrayList[PendingIntersect])

  private case class IntersectStats(redisTupleTotal: Long, redisTuplePassed: Long, redisAccessCount: Long)

  private val supportsContiguousSubtreeOrders: Boolean = mapper.metadata.contiguousSubtreeOrders
  // Bits reserved for the per-qOrder suffix in main-table row keys.
  private val mainTableMoveBits: Int = math.max(1, 64 - java.lang.Long.numberOfLeadingZeros(mapper.metadata.maxShapes))

  private val qOrderRoots: java.util.List[QOrderTreeNode] = buildQOrderTree()
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

  def similarityRanges(
                        searTraj: Trajectory,
                        dis: Double,
                        jedis: Jedis,
                        indexTable: String
                      ): java.util.List[IndexRange] = {
    similarityRangesWithNativeQOrderTree(searTraj, dis, jedis, indexTable)
  }

  private def similarityRangesWithNativeQOrderTree(
                                                    searTraj: Trajectory,
                                                    dis: Double,
                                                    jedis: Jedis,
                                                    indexTable: String
                                                  ): java.util.List[IndexRange] = {
    val boundary1 = searTraj.getMultiPoint.getEnvelopeInternal
    val boundaryEnv = searTraj.getMultiPoint.getEnvelopeInternal
    boundary1.expandBy(dis)

    val minimumResolution =
      resolutionMBR(boundary1.getMinX, boundary1.getMinY, boundary1.getMaxX, boundary1.getMaxY, alpha, beta)

    var maximumResolution = minimumResolution.length
    var currXS = minimumResolution.xWidth / 2.0
    var currYS = minimumResolution.yWidth / 2.0
    while ((boundaryEnv.getWidth - currXS) / 2.0 < dis
      && (boundaryEnv.getHeight - currYS) / 2.0 < dis
      && maximumResolution < maxR) {
      maximumResolution += 1
      currXS /= 2.0
      currYS /= 2.0
    }

    val unionMasks = LETILocSIndex.getCachedUnionMasks(jedis, indexTable)
    val expandedWindow = QueryWindow(
      boundary1.getMinX,
      boundary1.getMinY,
      boundary1.getMaxX,
      boundary1.getMaxY
    )
    val ranges = new java.util.ArrayList[IndexRange](100)
    val pendingIntersects = new java.util.ArrayList[PendingIntersect]()
    val remaining = new java.util.ArrayDeque[QOrderTreeNode](math.max(16, qOrderRoots.size()))
    val levelStop = QOrderTreeNode(
      -1L,
      -1,
      null,
      -1,
      -1L,
      0.0,
      0.0,
      0.0,
      0.0,
      0,
      0
    )

    qOrderRoots.asScala.foreach(remaining.add)
    remaining.add(levelStop)
    var level = if (qOrderRoots.isEmpty) 0 else qOrderRoots.get(0).level

    var containedQuadCount = 0L
    var intersectQuadCount = 0L

    while (!remaining.isEmpty) {
      val node = remaining.poll()
      if (node.eq(levelStop)) {
        if (!remaining.isEmpty && level < maximumResolution) {
          level += 1
          remaining.add(levelStop)
        }
      } else if (node.level <= maximumResolution && insertion(expandedWindow, node)) {
        if (node.level < minimumResolution.length) {
          node.children.asScala.foreach(remaining.add)
        } else if (neededToCheckSimilarity(boundaryEnv, dis, node)) {
          val signature = calculateSignature(expandedWindow, node)
          val unionMask = unionMasks.get(lang.Integer.valueOf(node.qOrder))
          if (unionMask == null || (signature & unionMask.longValue()) != 0L) {
            pendingIntersects.add(PendingIntersect(node, signature))
            intersectQuadCount += 1L
            if (isContained(expandedWindow, node)) {
              containedQuadCount += 1L
            }
          }
          if (node.level < maximumResolution) {
            node.children.asScala.foreach(remaining.add)
          }
        }
      }
    }

    val pp = jedis.pipelined()
    val intersectStats =
      try {
        processIntersectBatches(
          pendingIntersects,
          indexTable,
          pp,
          ranges,
          searTraj,
          dis,
          DebugState(intersectLimit = 0, resultLimit = 0)
        )
      } finally {
        pp.close()
        jedis.close()
      }

    val merged = mergeRanges(ranges)
    val redisShapeFilterRate =
      if (intersectStats.redisTupleTotal > 0L) {
        (intersectStats.redisTuplePassed * 100L) / intersectStats.redisTupleTotal
      } else {
        0L
      }

    LETILocSIndex.setLastRangeStats(
      LETILocSIndex.RangeStats(
        containedQuadCount,
        intersectQuadCount,
        intersectStats.redisAccessCount,
        redisShapeFilterRate
      )
    )
    merged
  }

  private def rangesWithNativeQOrderTree(
                                  lng1: Double,
                                  lat1: Double,
                                  lng2: Double,
                                  lat2: Double,
                                  jedis: Jedis,
                                  indexTable: String
                                ): java.util.List[IndexRange] = {
    val debugState = DebugState(intersectLimit = 20, resultLimit = 20)
    val unionMasks = LETILocSIndex.getCachedUnionMasks(jedis, indexTable)

    val queryWindow = QueryWindow(lng1, lat1, lng2, lat2)
    val ranges = new java.util.ArrayList[IndexRange](100)
    val quadCodeRanges = new java.util.ArrayList[IndexRange](256)
    val qOrderRanges = new java.util.ArrayList[IndexRange](256)

    val pp = jedis.pipelined()
    val traversal = collectCandidateNodes(
      queryWindow,
      unionMasks,
      ranges,
      quadCodeRanges,
      qOrderRanges,
      debugState
    )
    val intersectStats =
      try {
        processIntersectBatches(
          traversal.pendingIntersects,
          indexTable,
          pp,
          ranges,
          null,
          Double.NaN,
          debugState
        )
      } finally {
        pp.close()
        jedis.close()
      }

    val merged = mergeRanges(ranges)
    val redisShapeFilterRate =
      if (intersectStats.redisTupleTotal > 0L) {
        (intersectStats.redisTuplePassed * 100L) / intersectStats.redisTupleTotal
      } else {
        0L
      }

    LETILocSIndex.setLastRangeStats(
      LETILocSIndex.RangeStats(
        traversal.containedQuadCount.toLong,
        traversal.intersectQuadCount.toLong,
        intersectStats.redisAccessCount,
        redisShapeFilterRate
      )
    )
    merged
  }

  private def collectCandidateNodes(
                                     queryWindow: QueryWindow,
                                     unionMasks: java.util.Map[lang.Integer, lang.Long],
                                     ranges: java.util.ArrayList[IndexRange],
                                     quadCodeRanges: java.util.ArrayList[IndexRange],
                                     qOrderRanges: java.util.ArrayList[IndexRange],
                                     debugState: DebugState
                                   ): TraversalResult = {
    val remaining = new java.util.ArrayDeque[QOrderTreeNode](math.max(16, qOrderRoots.size()))
    qOrderRoots.asScala.foreach(remaining.add)

    var containedQuadCount = 0
    var intersectQuadCount = 0
    val pendingIntersects = new java.util.ArrayList[PendingIntersect]()

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
        enqueueIntersectNode(node, queryWindow, unionMasks, qOrderRanges, pendingIntersects, debugState)
        node.children.asScala.foreach(remaining.add)
      }
    }

    TraversalResult(containedQuadCount, intersectQuadCount, pendingIntersects)
  }

  private def enqueueIntersectNode(
                                    node: QOrderTreeNode,
                                    queryWindow: QueryWindow,
                                    unionMasks: java.util.Map[lang.Integer, lang.Long],
                                    qOrderRanges: java.util.ArrayList[IndexRange],
                                    pendingIntersects: java.util.ArrayList[PendingIntersect],
                                    debugState: DebugState
                                  ): Unit = {
    val signature = calculateSignature(queryWindow, node)
    val unionMask = unionMasks.get(lang.Integer.valueOf(node.qOrder))

    qOrderRanges.add(IndexRange(node.qOrder.toLong, node.qOrder.toLong, contained = false))

    if (unionMask != null && (signature & unionMask.longValue()) == 0L) {
      debugIntersectSkip(node, signature, unionMask.longValue(), debugState)
      return
    }

    pendingIntersects.add(PendingIntersect(node, signature))
    debugIntersectEnqueue(node, signature, debugState)
  }

  private def processIntersectBatches(
                                       pendingIntersects: java.util.List[PendingIntersect],
                                       indexTable: String,
                                       pipeline: redis.clients.jedis.Pipeline,
                                       ranges: java.util.ArrayList[IndexRange],
                                       queryTrajectory: Trajectory,
                                       threshold: Double,
                                       debugState: DebugState
                                     ): IntersectStats = {
    val lookups = buildIntersectLookups(pendingIntersects, indexTable)
    val redisAccessCount = queueIntersectPayloadFetches(lookups, indexTable, pipeline)
    pipeline.sync()

    var redisTupleTotal = 0L
    var redisTuplePassed = 0L

    val iterator = lookups.iterator()
    while (iterator.hasNext) {
      val lookup = iterator.next()
      val payload = loadShapePayload(lookup)
      val shapeCount = shapeCountFromPayload(payload)
      debugBatchFetch(lookup, shapeCount, debugState)
      val passed = appendMatchedRowKeys(lookup, payload, ranges, queryTrajectory, threshold)
      redisTupleTotal += shapeCount
      redisTuplePassed += passed
      debugBatchMatch(lookup, if (passed > 0L) 1 else 0, debugState)
    }

    IntersectStats(redisTupleTotal, redisTuplePassed, redisAccessCount)
  }

  private def buildIntersectLookups(
                                     pendingIntersects: java.util.List[PendingIntersect],
                                     indexTable: String
                                   ): java.util.ArrayList[IntersectLookup] = {
    val lookups = new java.util.ArrayList[IntersectLookup]()
    if (pendingIntersects == null || pendingIntersects.isEmpty) {
      return lookups
    }

    val iterator = pendingIntersects.iterator()
    while (iterator.hasNext) {
      val pending = iterator.next()
      lookups.add(
        IntersectLookup(
          pending.node,
          pending.signature,
          LETILocSIndex.shapeCacheKey(indexTable, pending.node.qOrder)
        )
      )
    }
    lookups
  }

  private def queueIntersectPayloadFetches(
                                            lookups: java.util.List[IntersectLookup],
                                            indexTable: String,
                                            pipeline: redis.clients.jedis.Pipeline
                                          ): Long = {
    var redisAccessCount = 0L
    val hashKey = LETILocSIndex.shapeHashKeyBytes(indexTable)
    val iterator = lookups.iterator()
    while (iterator.hasNext) {
      val lookup = iterator.next()
      val cachedPayload = LETILocSIndex.getCachedShapePayload(lookup.cacheKey)
      if (cachedPayload != null) {
        lookup.cachedPayload = cachedPayload
      } else {
        // Loader persists LETI shape payloads with an 8-byte long qOrder hash field.
        lookup.response = pipeline.hget(hashKey, Bytes.toBytes(lookup.node.qOrder.toLong))
        redisAccessCount += 1L
      }
    }
    redisAccessCount
  }

  private def loadShapePayload(lookup: IntersectLookup): Array[Byte] = {
    if (lookup.cachedPayload != null) {
      return lookup.cachedPayload
    }

    val loaded = lookup.response.get()
    if (loaded == null) {
      return null
    }

    LETILocSIndex.putCachedShapePayload(lookup.cacheKey, loaded)
    loaded
  }

  private def shapeCountFromPayload(payload: Array[Byte]): Int = {
    if (payload == null || payload.isEmpty) 0 else payload.length / java.lang.Long.BYTES
  }

  private def appendMatchedRowKeys(
                                    lookup: IntersectLookup,
                                    payload: Array[Byte],
                                    ranges: java.util.ArrayList[IndexRange],
                                    queryTrajectory: Trajectory,
                                    threshold: Double
                                  ): Long = {
    if (payload == null || payload.isEmpty) {
      return 0L
    }

    val abnormalCellMask =
      if (queryTrajectory == null || java.lang.Double.isNaN(threshold)) 0L
      else calculateAbnormalCellMask(lookup.node, queryTrajectory, threshold)
    var passed = 0L
    var shapeOrder = 0
    var offset = 0
    while (offset + java.lang.Long.BYTES <= payload.length) {
      val shapeCode = Bytes.toLong(payload, offset)
      val matchedBySignature = (lookup.signature & shapeCode) != 0L
      val matchedByDistance = abnormalCellMask == 0L || (shapeCode & abnormalCellMask) == 0L
      if (matchedBySignature && matchedByDistance) {
        val rowKey = (lookup.node.qOrder.toLong << mainTableMoveBits) | shapeOrder
        ranges.add(IndexRange(rowKey, rowKey, contained = false))
        passed += 1L
      }
      shapeOrder += 1
      offset += java.lang.Long.BYTES
    }
    passed
  }

  private def debugIntersectSkip(
                                  node: QOrderTreeNode,
                                  signature: Long,
                                  unionMask: Long,
                                  debugState: DebugState
                                ): Unit = {
    if (debugState.printedIntersect < debugState.intersectLimit) {
      println(
        s"[LETI native dbg] skip intersect by union-mask: qOrder=${node.qOrder}, quadCode=${node.quadCode}, signature=$signature, unionMask=$unionMask"
      )
      debugState.printedIntersect += 1
    }
  }

  private def debugIntersectEnqueue(
                                     node: QOrderTreeNode,
                                     signature: Long,
                                     debugState: DebugState
                                   ): Unit = {
    if (debugState.printedIntersect < debugState.intersectLimit) {
      println(
        s"[LETI native dbg] enqueue intersect: qOrder=${node.qOrder}, quadCode=${node.quadCode}, level=${node.level}, signature=$signature"
      )
      debugState.printedIntersect += 1
    }
  }

  private def debugBatchFetch(
                               lookup: IntersectLookup,
                               tupleCount: Int,
                               debugState: DebugState
                             ): Unit = {
    if (debugState.printedResult < debugState.resultLimit) {
      println(
        s"[LETI native dbg] process intersect qOrder=${lookup.node.qOrder}, redisTupleCount=$tupleCount"
      )
      debugState.printedResult += 1
    }
  }

  private def debugBatchMatch(
                               lookup: IntersectLookup,
                               matchedQOrders: Int,
                               debugState: DebugState
                             ): Unit = {
    if (matchedQOrders > 0 && debugState.printedResult < debugState.resultLimit) {
      println(
        s"[LETI native dbg] passed in qOrder=${lookup.node.qOrder}, passedQOrders=$matchedQOrders"
      )
      debugState.printedResult += 1
    }
  }

  private def calculateAbnormalCellMask(
                                         node: QOrderTreeNode,
                                         queryTrajectory: Trajectory,
                                         threshold: Double
                                       ): Long = {
    var abnormalMask = 0L
    val cellWidth = (node.eeXmax - node.eeXmin) / node.partitionAlpha.toDouble
    val cellHeight = (node.eeYmax - node.eeYmin) / node.partitionBeta.toDouble
    val thresholdSquared = threshold * threshold
    val queryCoordinates = queryTrajectory.getMultiPoint.getCoordinates

    var x = 0
    while (x < node.partitionAlpha) {
      var y = 0
      while (y < node.partitionBeta) {
        val minX = node.eeXmin + x * cellWidth
        val minY = node.eeYmin + y * cellHeight
        if (pointSetDistanceSquaredToRectangle(queryCoordinates, minX, minY, minX + cellWidth, minY + cellHeight) > thresholdSquared) {
          abnormalMask |= signatureBit(x, y, node.partitionBeta)
        }
        y += 1
      }
      x += 1
    }
    abnormalMask
  }

  private def pointSetDistanceSquaredToRectangle(
                                                  coordinates: Array[org.locationtech.jts.geom.Coordinate],
                                                  minX: Double,
                                                  minY: Double,
                                                  maxX: Double,
                                                  maxY: Double
                                                ): Double = {
    var minDistanceSquared = Double.PositiveInfinity
    var idx = 0
    while (idx < coordinates.length) {
      val coordinate = coordinates(idx)
      val dx =
        if (coordinate.x < minX) minX - coordinate.x
        else if (coordinate.x > maxX) coordinate.x - maxX
        else 0.0
      val dy =
        if (coordinate.y < minY) minY - coordinate.y
        else if (coordinate.y > maxY) coordinate.y - maxY
        else 0.0
      val distanceSquared = dx * dx + dy * dy
      if (distanceSquared == 0.0) {
        return 0.0
      }
      if (distanceSquared < minDistanceSquared) {
        minDistanceSquared = distanceSquared
      }
      idx += 1
    }
    minDistanceSquared
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

  private def emptyContainedIntervals(): LETILocSIndex.ContainedIntervals =
    LETILocSIndex.ContainedIntervals(
      new java.util.ArrayList[IndexRange](),
      new java.util.ArrayList[IndexRange]()
    )

  private def buildContainedIntervalsFromRange(
                                                startQOrder: Int,
                                                endQOrder: Int
                                              ): LETILocSIndex.ContainedIntervals = {
    val intervals = emptyContainedIntervals()
    if (endQOrder < startQOrder) {
      return intervals
    }

    appendContainedRunIntervals(startQOrder, endQOrder, intervals.qOrderRanges, intervals.rowKeyRanges)
    intervals
  }

  private def buildContainedIntervalsFromOrderList(
                                                    selfQOrder: Int,
                                                    descendantOrders: Array[Int]
                                                  ): LETILocSIndex.ContainedIntervals = {
    val intervals = emptyContainedIntervals()
    val allOrders = collectAllOrders(selfQOrder, descendantOrders)

    var runStart = allOrders(0)
    var previous = allOrders(0)
    var idx = 1
    while (idx < allOrders.length) {
      val current = allOrders(idx)
      if (current != previous + 1) {
        appendContainedRunIntervals(runStart, previous, intervals.qOrderRanges, intervals.rowKeyRanges)
        runStart = current
      }
      previous = current
      idx += 1
    }

    appendContainedRunIntervals(runStart, previous, intervals.qOrderRanges, intervals.rowKeyRanges)
    intervals
  }

  private def collectAllOrders(selfQOrder: Int, descendantOrders: Array[Int]): Array[Int] = {
    val descendantCount = if (descendantOrders == null) 0 else descendantOrders.length
    val allOrders = new Array[Int](descendantCount + 1)
    allOrders(0) = selfQOrder
    if (descendantCount > 0) {
      System.arraycopy(descendantOrders, 0, allOrders, 1, descendantCount)
    }
    java.util.Arrays.sort(allOrders)
    allOrders
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
  private def buildQOrderTree(): java.util.List[QOrderTreeNode] = {
    val roots = new java.util.ArrayList[QOrderTreeNode]()
    if (validQuadCodes == null || validQuadCodes.isEmpty) {
      return roots
    }

    val nodes = validQuadCodes.asScala.toSeq
      .flatMap(createTreeNode)
      .sortBy(_.quadCode)

    val stack = new java.util.ArrayDeque[QOrderTreeNode]()
    nodes.foreach { node =>
      while (!stack.isEmpty && !isDescendantOf(node, stack.peekLast())) {
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

  // The native tree keeps only effective LETI nodes. Absorbed descendants stay in
  // coverage metadata instead of becoming standalone tree nodes.
  private def createTreeNode(codeBox: lang.Long): Option[QOrderTreeNode] = {
    val code = codeBox.longValue()
    val order = quadCodeOrder.get(codeBox)
    val parent = quadCodeParentQuad.get(codeBox)
    if (order == null || parent == null) {
      None
    } else {
      val eeBounds = expandedBounds(parent)
      Some(
        QOrderTreeNode(
          quadCode = code,
          qOrder = order.intValue(),
          parentQuad = parent,
          level = parent.level,
          subtreeUpper = code + IS(parent.level),
          eeXmin = eeBounds._1,
          eeYmin = eeBounds._2,
          eeXmax = eeBounds._3,
          eeYmax = eeBounds._4,
          partitionAlpha = effectivePartitionAlpha(parent),
          partitionBeta = effectivePartitionBeta(parent)
        )
      )
    }
  }

  private def expandedBounds(parent: ParentQuad): (Double, Double, Double, Double) = {
    val cellWidth = parent.xmax - parent.xmin
    val cellHeight = parent.ymax - parent.ymin
    (
      parent.xmin,
      parent.ymin,
      parent.xmin + alpha * cellWidth,
      parent.ymin + beta * cellHeight
    )
  }

  private def effectivePartitionAlpha(parent: ParentQuad): Int =
    if (adaptivePartition) parent.alpha else alpha

  private def effectivePartitionBeta(parent: ParentQuad): Int =
    if (adaptivePartition) parent.beta else beta

  private def isDescendantOf(node: QOrderTreeNode, parent: QOrderTreeNode): Boolean =
    node.quadCode >= parent.quadCode && node.subtreeUpper <= parent.subtreeUpper

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

  private def neededToCheckSimilarity(
                                       trajEnv: org.locationtech.jts.geom.Envelope,
                                       threshold: Double,
                                       node: QOrderTreeNode
                                     ): Boolean = {
    val ee = new org.locationtech.jts.geom.Envelope(node.eeXmin, node.eeXmax, node.eeYmin, node.eeYmax)
    ee.expandBy(threshold)
    ee.contains(trajEnv)
  }

  // Signature bits mark the grid cells inside the expanded envelope that intersect
  // the query window. Redis union masks use the same layout.
  private def calculateSignature(qw: QueryWindow, node: QOrderTreeNode): Long = {
    var signature = 0L

    val gridCellWidth = (node.eeXmax - node.eeXmin) / node.partitionAlpha
    val gridCellHeight = (node.eeYmax - node.eeYmin) / node.partitionBeta

    val xStart = clampGridStart(qw.xmin, node.eeXmin, gridCellWidth)
    val xEnd = clampGridEnd(qw.xmax, node.eeXmin, gridCellWidth, node.partitionAlpha)

    val yStart = clampGridStart(qw.ymin, node.eeYmin, gridCellHeight)
    val yEnd = clampGridEnd(qw.ymax, node.eeYmin, gridCellHeight, node.partitionBeta)

    var x = xStart
    while (x < xEnd) {
      var y = yStart
      while (y < yEnd) {
        val minX = node.eeXmin + gridCellWidth * x
        val minY = node.eeYmin + gridCellHeight * y
        val maxX = minX + gridCellWidth
        val maxY = minY + gridCellHeight

        if (qw.insertion(minX, minY, maxX, maxY)) {
          signature |= signatureBit(x, y, node.partitionBeta)
        }
        y += 1
      }
      x += 1
    }
    signature
  }

  private def clampGridStart(value: Double, min: Double, step: Double): Int =
    math.max(0, math.floor((value - min) / step).toInt)

  private def clampGridEnd(value: Double, min: Double, step: Double, exclusiveUpper: Int): Int =
    math.min(exclusiveUpper, math.floor((value - min) / step).toInt + 1)

  private def signatureBit(x: Int, y: Int, partitionBeta: Int): Long =
    1L << (x * partitionBeta + y)

  // Merge overlapping or adjacent row-key intervals before scanning the main table.
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

  private val maxShapeCacheSize: Int =
    java.lang.Integer.getInteger("tman.leti.shapeCacheSize", 200000)

  private val shapePayloadCache =
    new java.util.LinkedHashMap[String, Array[Byte]](16, 0.75f, true) {
      override def removeEldestEntry(
                                      eldest: java.util.Map.Entry[String, Array[Byte]]
                                    ): Boolean = {
        this.size() > maxShapeCacheSize
      }
    }

  private def withCacheLock[T](f: => T): T = shapePayloadCache.synchronized {
    f
  }

  private val unionMaskCache =
    new java.util.concurrent.ConcurrentHashMap[String, java.util.Map[lang.Integer, lang.Long]]()

  private def shapeHashKey(indexTable: String): String =
    indexTable + ":leti:shapes"

  private def shapeHashKeyBytes(indexTable: String): Array[Byte] =
    Bytes.toBytes(shapeHashKey(indexTable))

  private def shapeCacheKey(indexTable: String, qOrder: Int): String =
    indexTable + ":" + qOrder

  private def unionMaskKey(indexTable: String): String =
    indexTable + ":leti:union_mask"

  private def getCachedShapePayload(
                                     key: String
                                   ): Array[Byte] = withCacheLock {
    shapePayloadCache.get(key)
  }

  private def putCachedShapePayload(
                                     key: String,
                                     payload: Array[Byte]
                                   ): Unit = withCacheLock {
    shapePayloadCache.put(key, payload)
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

  // Lightweight query-side stats that Java experiments read after each query.
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
      shapePayloadCache.clear()
    }
    unionMaskCache.clear()
    cache.clear()
    cacheBounds.clear()
  }

  // Cache for instances that use the default world bounds.
  private val cache =
    new java.util.concurrent.ConcurrentHashMap[(String, Short, Int, Int, Boolean, String), LETILocSIndex]()

  // Cache for instances that use caller-provided bounds.
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
