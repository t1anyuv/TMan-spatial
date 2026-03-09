package index

import utils.LSFCReader
import utils.LSFCReader.{ParentQuad, loadFromClasspath}
import org.apache.hadoop.hbase.util.Bytes
import org.locationtech.sfcurve.IndexRange
import redis.clients.jedis.Jedis

import java.{lang, util}
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

class LETILocSIndex(
    maxR: Short,
    xBounds: (Double, Double),
    yBounds: (Double, Double),
    alpha: Int,
    beta: Int,
    adaptivePartition: Boolean = false
) extends LocSIndex(maxR, xBounds, yBounds, alpha, beta)
    with Serializable {

  private val mapper: LSFCReader.LSFCMapper = loadFromClasspath("RLOrder.json")

  private val quadCodeOrder: util.Map[lang.Long, Integer] = mapper.quadCodeOrder

  private val quadCodeParentQuad: util.Map[lang.Long, ParentQuad] = mapper.quadCodeParentQuad

  val validQuadCodes: util.Set[lang.Long] = mapper.validQuadCodes

  /**
   * 最大形状位数，用于统一编码空间
   * 自适应划分时从 RLOrder.json 的 metadata 获取，否则使用全局 alpha * beta
   */
  private val metaMaxShapeBits: Int = if (mapper.metadata != null) mapper.metadata.maxShapeBits else alpha * beta

  /**
   * 形状编码的位数
   * 自适应划分时使用 maxShapeBits，否则使用全局 alpha * beta
   */
  private val moveBits: Int = if (adaptivePartition && metaMaxShapeBits > 0) metaMaxShapeBits else alpha * beta

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
    val queryWindow = QueryWindow(lng1, lat1, lng2, lat2)

    val ranges = new java.util.ArrayList[IndexRange](100)
    val remaining = new java.util.ArrayDeque[TShapeEE](200)
    val allContainedOrders = new java.util.ArrayList[java.lang.Integer](1000)

    val pp = jedis.pipelined()
    case class IntersectTask(
        qOrder: Int,
        signature: Long,
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

      val minScore = quadOrder.toLong << moveBits
      val maxScore = ((quadOrder + 1L) << moveBits) - 1L
      val response = pp.zrangeByScoreWithScores(indexTable, minScore, maxScore)
      intersectTasks.add(IntersectTask(quadOrder.toInt, signature, response))
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
        val result = task.response.get()
        if (result != null) {
          val it = result.iterator()
          while (it.hasNext) {
            val elem = it.next()
            val payload = elem.getBinaryElement

            val originIndex = Bytes.toLong(payload, 8)
            val shapeCode = originIndex & ((1L << moveBits) - 1)

            // 签名匹配：signature=0 表示精度问题，直接放行；否则检查 shapeCode 是否与查询签名相交
            if (task.signature == 0 || (task.signature & shapeCode) != 0) {
              val shapeOrder = Bytes.toInt(payload, 4)
              val rowKey = (task.qOrder.toLong << moveBits) | shapeOrder
              ranges.add(IndexRange(rowKey, rowKey, contained = false))
            }
          }
        }
      }
    }
    while (!remaining.isEmpty) {
      val quad = remaining.poll()
      val quadCode = quad.elementCode
      if (validQuadCodes.contains(quadCode)) {
        if (quad.isContained(queryWindow)) {
          quad.collectOrders(validQuadCodes, quadCodeOrder, allContainedOrders)
        } else if (quad.insertion(queryWindow)) {
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

    if (!allContainedOrders.isEmpty) {
      ranges.addAll(mergeContainRanges(allContainedOrders))
    }

    pp.close()
    jedis.close()

    mergeRanges(ranges)
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
  private def mergeContainRanges(childrenOrders: util.List[Integer]): java.util.List[IndexRange] = {

    val orders = new Array[Int](childrenOrders.size())

    var idx = 0
    val it = childrenOrders.iterator()
    while (it.hasNext) {
      orders(idx) = it.next().intValue()
      idx += 1
    }

    val result: java.util.ArrayList[IndexRange] = new java.util.ArrayList[IndexRange]()

    if (orders.nonEmpty) {
      java.util.Arrays.sort(orders)

      var start = orders(0)
      var prev = start

      var i = 1
      while (i < orders.length) {
        val cur = orders(i)
        if (cur == prev + 1) {
          prev = cur
        } else {
          result.add(
            IndexRange(
              start.toLong << moveBits,
              ((prev.toLong + 1) << moveBits) - 1L,
              contained = true
            )
          )
          start = cur
          prev = cur
        }
        i += 1
      }

      result.add(
        IndexRange(start.toLong << moveBits, ((prev.toLong + 1) << moveBits) - 1L, contained = true)
      )
    }

    result
  }

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
  /**
   * 缓存 LETILocSIndex 实例（使用默认边界：-180~180, -90~90）
   */
  private val cache =
    new java.util.concurrent.ConcurrentHashMap[(String, Short, Int, Int, Boolean), LETILocSIndex]()

  /**
   * 缓存 LETILocSIndex 实例（使用自定义边界）
   */
  private val cacheBounds = new java.util.concurrent.ConcurrentHashMap[
    (String, Short, Int, Int, (Double, Double), (Double, Double), Boolean),
    LETILocSIndex
  ]()

  def apply(g: Short, alpha: Int, beta: Int): LETILocSIndex = {
    apply(g, alpha, beta, adaptivePartition = false)
  }

  def apply(g: Short, alpha: Int, beta: Int, adaptivePartition: Boolean): LETILocSIndex = {
    var sfc = cache.get(("", g, alpha, beta, adaptivePartition))
    if (sfc == null) {
      sfc = new LETILocSIndex(g, (-180.0, 180.0), (-90.0, 90.0), alpha, beta, adaptivePartition)
      cache.put(("", g, alpha, beta, adaptivePartition), sfc)
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
    var sfc = cacheBounds.get(("", g, alpha, beta, xBounds, yBounds, adaptivePartition))
    if (sfc == null) {
      sfc = new LETILocSIndex(g, xBounds, yBounds, alpha, beta, adaptivePartition)
      cacheBounds.put(("", g, alpha, beta, xBounds, yBounds, adaptivePartition), sfc)
    }
    sfc
  }

  def apply(table: String, g: Short, alpha: Int, beta: Int): LETILocSIndex = {
    apply(table, g, alpha, beta, adaptivePartition = false)
  }

  def apply(table: String, g: Short, alpha: Int, beta: Int, adaptivePartition: Boolean): LETILocSIndex = {
    var sfc = cache.get((table, g, alpha, beta, adaptivePartition))
    if (sfc == null) {
      sfc = new LETILocSIndex(g, (-180.0, 180.0), (-90.0, 90.0), alpha, beta, adaptivePartition)
      cache.put((table, g, alpha, beta, adaptivePartition), sfc)
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
    var sfc = cacheBounds.get((table, g, alpha, beta, xBounds, yBounds, adaptivePartition))
    if (sfc == null) {
      sfc = new LETILocSIndex(g, xBounds, yBounds, alpha, beta, adaptivePartition)
      cacheBounds.put((table, g, alpha, beta, xBounds, yBounds, adaptivePartition), sfc)
    }
    sfc
  }
}
