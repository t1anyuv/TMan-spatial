package index

import com.esri.core.geometry._
import org.locationtech.sfcurve.IndexRange
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

/**
 * 双 IndexRange 包装类
 * 
 * @param queryRange 用于实际查询的范围（lower=0，保证不漏检）
 * @param sortRange 用于排序和合并计算的范围（lower=minSFC，用于优化）
 */
case class DualIndexRange(
    queryRange: IndexRange,  // 实际查询使用
    sortRange: IndexRange    // 排序合并使用
) {
    // 便捷访问方法
    def queryLower: Long = queryRange.lower
    def queryUpper: Long = queryRange.upper
    def sortLower: Long = sortRange.lower
    def sortUpper: Long = sortRange.upper
}

object DualIndexRange {
    /**
     * 按 sortRange.lower 排序
     */
    val OrderBySortRange: java.util.Comparator[DualIndexRange] = 
        new java.util.Comparator[DualIndexRange] {
            override def compare(o1: DualIndexRange, o2: DualIndexRange): Int = {
                java.lang.Long.compare(o1.sortLower, o2.sortLower)
            }
        }
}

/**
 * LMSFC (Linearized Multi-Scale Space-Filling Curve) 索引实现
 *
 * @param maxR           最大分辨率（兼容XZSFC基类）
 * @param xBounds        x轴边界 (min, max)
 * @param yBounds        y轴边界 (min, max)
 * @param thetaConfigStr θ参数配置字符串（逗号分隔）
 */
class LMSFCIndex(
                  maxR: Short,
                  xBounds: (Double, Double),
                  yBounds: (Double, Double),
                  thetaConfigStr: String
                ) extends XZSFC(maxR, xBounds, yBounds, 2, 2) with Serializable {

  // 空间边界
  private val xLo = xBounds._1
  private val xHi = xBounds._2
  private val yLo = yBounds._1
  private val yHi = yBounds._2

  // 空间尺寸
  override val xSize: Double = xHi - xLo
  override val ySize: Double = yHi - yLo

  // 解析θ参数配置
  private val thetaConfig: Array[Int] = thetaConfigStr.split(",").map(_.trim.toInt)

  // θ参数分解
  private val bitNum = thetaConfig.length / 2
  validateThetaConfig(thetaConfig)

  // 网格大小
  private val gridSize: Int = 1 << bitNum

  private val thetaX: Array[Long] = thetaConfig.slice(0, bitNum).map(i => 1L << i)
  private val thetaY: Array[Long] = thetaConfig.slice(bitNum, thetaConfig.length).map(i => 1L << i)

  /**
   * 验证 thetaConfig 是否满足论文中的三个约束：
   * (1) theta 为 2 的幂 (在你的代码中通过 1L << i 隐式满足，这里检查 i 的范围)
   * (2) 所有 theta 互不相同 (保证双射 Bijective)
   * (3) 同一维度内，低位 bit 对应的 theta 必须小于高位 bit 对应的 theta (保证单调性 Monotonic)
   */
  def validateThetaConfig(config: Array[Int]): Unit = {
    // 约束 1 & 2: 所有的位偏移量必须在合法范围内 (0-62) 且互不重复
    require(config.length == bitNum * 2, s"配置长度必须为 ${bitNum * 2}")
    require(config.forall(i => i >= 0 && i < 63), "位偏移量必须在 0 到 62 之间")
    require(config.distinct.length == config.length, "存在重复的位偏移量，将导致非双射映射（冲突）")

    // 分解维度
    val thetaXIdx = config.slice(0, bitNum)
    val thetaYIdx = config.slice(bitNum, config.length)

    // 约束 3: 核心单调性检查
    // 对于维度 i，如果 j < j'，那么 theta_j < theta_j'
    // 换句话说，存储在数组中的位权重索引必须是严格递增的
    for (i <- 0 until bitNum - 1) {
      require(thetaXIdx(i) < thetaXIdx(i + 1),
        s"X维度不满足单调性：索引 $i (${thetaXIdx(i)}) 必须小于索引 ${i + 1} (${thetaXIdx(i + 1)})")

      require(thetaYIdx(i) < thetaYIdx(i + 1),
        s"Y维度不满足单调性：索引 $i (${thetaYIdx(i)}) 必须小于索引 ${i + 1} (${thetaYIdx(i + 1)})")
    }

    println("Theta 配置校验通过：满足双射与单调性约束。")
  }

  /**
   * 将实际坐标归一化到网格坐标（内联优化）
   */
  @inline
  private def normalizeToGrid(value: Double, min: Double, max: Double): Int = {
    val normalized = (value - min) / (max - min)
    math.min((normalized * gridSize).toInt, gridSize - 1)
  }

  /**
   * 计算SFC位置编号
   */
  private def calcSFCPosition(x: Int, y: Int): Long = {
    var xValue = 0L
    var yValue = 0L
    var i = 0
    while (i < bitNum) {
      xValue += ((x >> i) & 1) * thetaX(i)
      yValue += ((y >> i) & 1) * thetaY(i)
      i += 1
    }
    xValue + yValue
  }

  /**
   * 为几何体创建LMSFC索引
   */
  override def index(geometry: Geometry, lenient: Boolean = false): (Int, Long, Long) = {
    val mbr = new Envelope2D()
    geometry.queryEnvelope2D(mbr)

    val minX = normalizeToGrid(mbr.xmin, xLo, xHi)
    val minY = normalizeToGrid(mbr.ymin, yLo, yHi)
    val maxX = normalizeToGrid(mbr.xmax, xLo, xHi)
    val maxY = normalizeToGrid(mbr.ymax, yLo, yHi)

    val minSFC = calcSFCPosition(minX, minY)
    val maxSFC = calcSFCPosition(maxX, maxY)

    (0, minSFC, maxSFC)
  }

  /**
   * 基础范围查询（从 0 开始，保证不漏检）
   * 利用Z曲线的单调性：矩形区域内最小值为左下角，最大值为右上角
   */
  def ranges(
              lng1: Double,
              lat1: Double,
              lng2: Double,
              lat2: Double
            ): java.util.List[IndexRange] = {
    val qMinX = normalizeToGrid(lng1, xLo, xHi)
    val qMinY = normalizeToGrid(lat1, yLo, yHi)
    val qMaxX = normalizeToGrid(lng2, xLo, xHi)
    val qMaxY = normalizeToGrid(lat2, yLo, yHi)

    // 利用Z曲线单调性：左下角是最小值，右上角是最大值
    val minSFC = calcSFCPosition(qMinX, qMinY)
    val maxSFC = calcSFCPosition(qMaxX, qMaxY)

    val ranges = new java.util.ArrayList[IndexRange]()
    // 从 0 开始，保证不漏检
    ranges.add(IndexRange(0, maxSFC, contained = false))
    ranges
  }

  /**
   * 返回双 IndexRange（用于查询分割）
   * 
   * @return DualIndexRange(queryRange, sortRange)
   *         - queryRange: IndexRange(0, maxSFC) 用于实际查询
   *         - sortRange: IndexRange(minSFC, maxSFC) 用于排序合并
   */
  private def dualRanges(
      lng1: Double,
      lat1: Double,
      lng2: Double,
      lat2: Double
  ): DualIndexRange = {
    val qMinX = normalizeToGrid(lng1, xLo, xHi)
    val qMinY = normalizeToGrid(lat1, yLo, yHi)
    val qMaxX = normalizeToGrid(lng2, xLo, xHi)
    val qMaxY = normalizeToGrid(lat2, yLo, yHi)
    
    val minSFC = calcSFCPosition(qMinX, qMinY)
    val maxSFC = calcSFCPosition(qMaxX, qMaxY)
    
    DualIndexRange(
        queryRange = IndexRange(0, maxSFC, contained = false),      // 实际查询：从 0 开始
        sortRange = IndexRange(minSFC, maxSFC, contained = false)   // 排序合并：从 minSFC 开始
    )
  }

  /**
   * 优化范围查询（带查询分割）
   * 
   * 使用双 IndexRange 策略：
   * - 排序和合并使用 sortRange (minSFC, maxSFC)
   * - 实际查询使用 queryRange (0, maxSFC)
   * 
   * @param lng1 查询窗口左下角经度
   * @param lat1 查询窗口左下角纬度
   * @param lng2 查询窗口右上角经度
   * @param lat2 查询窗口右上角纬度
   * @param kMax 最大分割深度
   * @return 用于实际查询的 IndexRange 列表（lower=0）
   */
  def rangesWithSplit(
                       lng1: Double,
                       lat1: Double,
                       lng2: Double,
                       lat2: Double,
                       kMax: Int = 3
                     ): java.util.List[IndexRange] = {
    // 1. 查询分割：生成子查询
    val subQueries = recursiveQuerySplitting(lng1, lat1, lng2, lat2, kMax)

    // 2. 为每个子查询生成双 IndexRange
    val allDualRanges = new java.util.ArrayList[DualIndexRange]()
    for (subQuery <- subQueries.asScala) {
      val dualRange = dualRanges(subQuery._1, subQuery._2, subQuery._3, subQuery._4)
      allDualRanges.add(dualRange)
    }

    if (allDualRanges.size() > 1) {
      // 3. 按 sortRange.lower 排序
      allDualRanges.sort(DualIndexRange.OrderBySortRange)
      
      val result = ArrayBuffer.empty[IndexRange]
      var current = allDualRanges.get(0)
      var i = 1
      
      // 4. 使用 sortRange 判断是否合并
      while (i < allDualRanges.size()) {
        val next = allDualRanges.get(i)
        
        // 关键：使用 sortRange 判断相邻性
        if (next.sortLower <= current.sortUpper + 1) {
          // 相邻或重叠，合并
          current = DualIndexRange(
            // queryRange: 合并 queryRange
            queryRange = IndexRange(
              current.queryLower,  // 保持 0
              math.max(current.queryUpper, next.queryUpper),
              current.queryRange.contained && next.queryRange.contained
            ),
            // sortRange: 合并 sortRange
            sortRange = IndexRange(
              current.sortLower,  // 保持第一个的 minSFC
              math.max(current.sortUpper, next.sortUpper),
              current.sortRange.contained && next.sortRange.contained
            )
          )
        } else {
          // 不相邻，保存当前的 queryRange（用于实际查询）
          result.append(current.queryRange)
          current = next
        }
        i += 1
      }
      // 保存最后一个的 queryRange
      result.append(current.queryRange)
      result.asJava
    } else {
      // 只有一个范围，返回 queryRange
      val resultList = new java.util.ArrayList[IndexRange]()
      resultList.add(allDualRanges.get(0).queryRange)
      resultList
    }
  }

  /**
   * 递归查询分割算法
   */
  private def recursiveQuerySplitting(
                                       lng1: Double,
                                       lat1: Double,
                                       lng2: Double,
                                       lat2: Double,
                                       kMax: Int
                                     ): java.util.List[(Double, Double, Double, Double)] = {
    val splits = new java.util.ArrayList[(Double, Double, Double, Double)]()

    def split(xMin: Int, xMax: Int, yMin: Int, yMax: Int, k: Int): Unit = {
      if (k == 0 || (xMin == xMax && yMin == yMax)) {
        val lng1Real = xLo + (xMin.toDouble / gridSize) * xSize
        val lat1Real = yLo + (yMin.toDouble / gridSize) * ySize
        val lng2Real = xLo + (xMax.toDouble / gridSize) * xSize
        val lat2Real = yLo + (yMax.toDouble / gridSize) * ySize
        splits.add((lng1Real, lat1Real, lng2Real, lat2Real))
        return
      }

      var maxBenefit = Long.MinValue
      var bestV = -1
      var bestDelta = -1

      for (delta <- 0 to 1) {
        val v = getV(xMin, xMax, yMin, yMax, delta)
        if (v != -1) {
          val benefit = calcBenefit(xMin, xMax, yMin, yMax, delta, v)
          if (benefit > maxBenefit) {
            maxBenefit = benefit
            bestV = v
            bestDelta = delta
          }
        }
      }

      if (bestDelta == 0) {
        split(xMin, bestV - 1, yMin, yMax, k - 1)
        split(bestV, xMax, yMin, yMax, k - 1)
      } else if (bestDelta == 1) {
        split(xMin, xMax, yMin, bestV - 1, k - 1)
        split(xMin, xMax, bestV, yMax, k - 1)
      } else {
        val lng1Real = xLo + (xMin.toDouble / gridSize) * xSize
        val lat1Real = yLo + (yMin.toDouble / gridSize) * ySize
        val lng2Real = xLo + (xMax.toDouble / gridSize) * xSize
        val lat2Real = yLo + (yMax.toDouble / gridSize) * ySize
        splits.add((lng1Real, lat1Real, lng2Real, lat2Real))
      }
    }

    val qMinX = normalizeToGrid(lng1, xLo, xHi)
    val qMinY = normalizeToGrid(lat1, yLo, yHi)
    val qMaxX = normalizeToGrid(lng2, xLo, xHi)
    val qMaxY = normalizeToGrid(lat2, yLo, yHi)

    split(qMinX, qMaxX, qMinY, qMaxY, kMax)
    splits
  }

  /**
   * 计算分割点v（内联优化）
   */
  @inline
  private def getV(xMin: Int, xMax: Int, yMin: Int, yMax: Int, delta: Int): Int = {
    val (qL, qU) = if (delta == 0) (xMin, xMax) else (yMin, yMax)
    val xorValue = qL ^ qU
    if (xorValue == 0) return -1

    val l = 31 - Integer.numberOfLeadingZeros(xorValue)
    if (l < 0) return -1

    val v = (qU >> l) << l
    v
  }

  /**
   * 计算分割收益（内联优化）
   */
  @inline
  private def calcBenefit(
                           xMin: Int,
                           xMax: Int,
                           yMin: Int,
                           yMax: Int,
                           delta: Int,
                           v: Int
                         ): Long = {
    val (uX, uY, lX, lY) = if (delta == 0) {
      (v - 1, yMax, v, yMin)
    } else {
      (xMax, v - 1, xMin, v)
    }

    val fU = calcSFCPosition(uX, uY)
    val fL = calcSFCPosition(lX, lY)

    fL - fU
  }
}

object LMSFCIndex extends Serializable {
  private val cache = new java.util.concurrent.ConcurrentHashMap[
    (String, Short, (Double, Double), (Double, Double), String),
    LMSFCIndex
  ]()

  def apply(
             g: Short,
             xBounds: (Double, Double),
             yBounds: (Double, Double),
             thetaConfigStr: String
           ): LMSFCIndex = {
    val key = ("", g, xBounds, yBounds, thetaConfigStr)
    var sfc = cache.get(key)
    if (sfc == null) {
      sfc = new LMSFCIndex(g, xBounds, yBounds, thetaConfigStr)
      cache.put(key, sfc)
    }
    sfc
  }

  def apply(
             table: String,
             g: Short,
             xBounds: (Double, Double),
             yBounds: (Double, Double),
             thetaConfigStr: String
           ): LMSFCIndex = {
    val key = (table, g, xBounds, yBounds, thetaConfigStr)
    var sfc = cache.get(key)
    if (sfc == null) {
      sfc = new LMSFCIndex(g, xBounds, yBounds, thetaConfigStr)
      cache.put(key, sfc)
    }
    sfc
  }
}
