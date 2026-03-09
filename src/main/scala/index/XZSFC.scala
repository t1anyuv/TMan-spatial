package index

import com.esri.core.geometry._
import entity.Trajectory
import org.locationtech.jts.geom
import org.locationtech.jts.geom.{Coordinate, GeometryFactory, PrecisionModel}
import org.locationtech.sfcurve.IndexRange

import java.util

/**
 * XZ Space-Filling Curve (XZ SFC) 实现
 * 用于空间索引和查询，支持四叉树编码和空间填充曲线
 *
 * @param maxR    最大四叉树层级
 * @param xBounds x 轴边界 (min, max)
 * @param yBounds y 轴边界 (min, max)
 * @param alpha   x 方向的网格数量
 * @param beta    y 方向的网格数量
 */
class XZSFC(maxR: Short, xBounds: (Double, Double), yBounds: (Double, Double), alpha: Int, beta: Int) {
  // 空间边界
  private val xLo = xBounds._1
  private val xHi = xBounds._2
  private val yLo = yBounds._1
  private val yHi = yBounds._2

  // 空间尺寸
  val xSize: Double = xHi - xLo
  val ySize: Double = yHi - yLo

  /**
   * 计算四叉树编码
   * 根据给定的点坐标和层级，计算该点所在四叉树节点的编码和边界
   *
   * @param x 点的 x 坐标
   * @param y 点的 y 坐标
   * @param l 四叉树层级
   * @return (编码, 左下角x, 左下角y, 右上角x, 右上角y)
   */
  def sequenceCode(x: Double, y: Double, l: Int): (Long, Double, Double, Double, Double) = {
    var i = 1
    var xmin = xLo
    var ymin = yLo
    var xmax = xHi
    var ymax = yHi
    var cs = 0L // 四叉树编码
    // 逐层向下，根据点所在象限更新编码和边界
    while (i <= l) {
      val xCenter = (xmin + xmax) / 2.0
      val yCenter = (ymin + ymax) / 2.0
      (x < xCenter, y < yCenter) match {
        case (true, true) => // 左下象限 (0)
          cs += 1L
          xmax = xCenter
          ymax = yCenter
        case (false, true) => // 右下象限 (1)
          cs += 1L + 1L * IS(i)
          xmin = xCenter
          ymax = yCenter
        case (true, false) => // 左上象限 (2)
          cs += 1L + 2L * IS(i)
          xmax = xCenter
          ymin = yCenter
        case (false, false) => // 右上象限 (3)
          cs += 1L + 3L * IS(i)
          xmin = xCenter
          ymin = yCenter
      }
      i += 1
    }
    (cs, xmin, ymin, xmax, ymax)
  }

  /**
   * 计算层级 i 的间隔大小 (Interval Size)
   * 用于计算四叉树编码中不同象限的偏移量
   *
   * @param i 层级
   * @return 该层级的间隔大小
   */
  def IS(i: Int): Long = {
    (math.pow(4, maxR - i + 1).toLong - 1L) / 3L
  }

  /**
   * 计算 MBR 的分辨率和索引参数
   * 根据 MBR 的大小和网格数量，计算合适的四叉树层级和索引参数
   *
   * @param xmin   MBR 最小 x 坐标
   * @param ymin   MBR 最小 y 坐标
   * @param xmax   MBR 最大 x 坐标
   * @param ymax   MBR 最大 y 坐标
   * @param xCells x 方向的网格数量
   * @param yCells y 方向的网格数量
   * @return 索引参数，包含编码、层级、宽度、边界等信息
   */
  def resolutionMBR(xmin: Double, ymin: Double, xmax: Double, ymax: Double, xCells: Int, yCells: Int): IndexPar = {
    // 计算最大维度比例，用于确定合适的层级
    val maxDim = math.max(((xmax - xmin) / xSize) / xCells.toDouble, ((ymax - ymin) / ySize) / yCells.toDouble)
    var l = math.floor(math.log(maxDim) / math.log(0.5)).toInt

    // 限制层级在有效范围内
    if (l > maxR) {
      l = maxR
    } else {
      val w = xSize * math.pow(0.5, l) // 该层级的网格宽度
      val h = ySize * math.pow(0.5, l) // 该层级的网格高度

      // 检查 MBR 是否能够完全包含在网格内
      def predicateX(min: Double, max: Double): Boolean =
        max <= xLo + (math.floor((min - xLo) / w) * w) + (xCells * w)

      def predicateY(min: Double, max: Double): Boolean =
        max <= yLo + (math.floor((min - yLo) / h) * h) + (yCells * h)

      // 如果 MBR 超出网格范围，降低一层
      if (!predicateX(xmin, xmax) || !predicateY(ymin, ymax)) {
        l = l - 1
      }
    }
    if (l < 0) {
      l = 0
    }

    // 计算四叉树编码
    val sc = sequenceCode(xmin, ymin, l)
    val w = math.pow(0.5, l) * xSize
    val h = math.pow(0.5, l) * ySize

    // 对齐到网格边界
    val x = math.floor(xmin / w) * w
    val y = math.floor(ymin / h) * h
    val xWidth = w
    val yWidth = h
    val xTrue = x + xLo
    val yTrue = y + yLo
    val xMaxTrue = xTrue + w * alpha.toDouble
    val yMaxTrue = yTrue + h * beta.toDouble
    val xCen = (xTrue + xMaxTrue) / 2.0
    val yCen = (yTrue + yMaxTrue) / 2.0
    IndexPar(sc._1, l, xWidth, yWidth, xTrue, yTrue, xMaxTrue, yMaxTrue, xCen, yCen)
  }

  /**
   * 索引参数，包含编码、层级、尺寸和边界信息
   */
  case class IndexPar(
                       var sc: Long, // 四叉树编码
                       var length: Int, // 层级
                       var xWidth: Double, // x 方向网格宽度
                       var yWidth: Double, // y 方向网格宽度
                       var xTrue: Double, // 真实 x 最小值
                       var yTrue: Double, // 真实 y 最小值
                       var xMaxTrue: Double, // 真实 x 最大值
                       var yMaxTrue: Double, // 真实 y 最大值
                       var xCen: Double, // x 中心点
                       var yCen: Double // y 中心点
                     )

  /**
   * 计算 MBR 的合适分辨率层级
   * 简化版本，只返回层级，不返回完整的索引参数
   *
   * @param mbr    最小边界矩形
   * @param xCells x 方向的网格数量
   * @param yCells y 方向的网格数量
   * @return 合适的四叉树层级
   */
  def resolutionMBR(mbr: Envelope2D, xCells: Int, yCells: Int): Int = {
    val maxDim = math.max(((mbr.xmax - mbr.xmin) / xSize) / xCells.toDouble, ((mbr.ymax - mbr.ymin) / ySize) / yCells.toDouble)
    var l = math.floor(math.log(maxDim) / math.log(0.5)).toInt

    if (l > maxR) {
      l = maxR
    } else {
      val w = xSize * math.pow(0.5, l)
      val h = ySize * math.pow(0.5, l)

      def predicateX(min: Double, max: Double): Boolean =
        max <= xLo + (math.floor((min - xLo) / w) * w) + (xCells * w)

      def predicateY(min: Double, max: Double): Boolean =
        max <= yLo + (math.floor((min - yLo) / h) * h) + (yCells * h)

      if (!predicateX(mbr.xmin, mbr.xmax) || !predicateY(mbr.ymin, mbr.ymax)) {
        l = l - 1
      }
    }
    if (l < 0) {
      l = 0
    }
    l
  }

  /**
   * 计算几何体的合适分辨率层级
   *
   * @param geometry 几何体
   * @param xCells   x 方向的网格数量
   * @param yCells   y 方向的网格数量
   * @return 合适的四叉树层级
   */
  def resolution(geometry: Geometry, xCells: Int, yCells: Int): Int = {
    val mbr = new Envelope2D()
    geometry.queryEnvelope2D(mbr)
    resolutionMBR(mbr, xCells, yCells)
  }

  /**
   * 为几何体创建索引
   * 自动计算合适的层级
   *
   * @param geometry 几何体
   * @param lenient  是否宽松模式（允许超出边界）
   * @return (层级, 位置编码, 签名)
   */
  def index(geometry: Geometry, lenient: Boolean = false): (Int, Long, Long) = {
    val mbr = new Envelope2D()
    geometry.queryEnvelope2D(mbr)
    val l = resolution(geometry, alpha, beta)
    index(geometry, l)
  }

  /**
   * 为几何体创建索引（指定层级）
   *
   * @param geometry 几何体
   * @param l        四叉树层级
   * @return (层级, 位置编码, 签名)
   */
  def index(geometry: Geometry, l: Int): (Int, Long, Long) = {
    val mbr = new Envelope2D()
    geometry.queryEnvelope2D(mbr)
    val w = xSize * math.pow(0.5, l) // 该层级的网格宽度
    val h = ySize * math.pow(0.5, l) // 该层级的网格高度
    val sCode = sequenceCode(mbr.xmin, mbr.ymin, l)
    val location = sCode._1 // 四叉树位置编码
    val x = sCode._2 // 网格左下角 x
    val y = sCode._3 // 网格左下角 y

    // 计算签名：表示几何体与哪些网格单元相交
    val sig = signature(x, y, w, h, geometry)
    (l, location, sig)
  }

  /**
   * 根据位置编码获取 MBR 和网格范围
   * 将位置编码解码为空间边界，并扩展到指定的网格数量
   *
   * @param location 四叉树位置编码
   * @param xCell    x 方向的网格数量
   * @param yCells   y 方向的网格数量
   * @return (xmin, ymin, xmax, ymax) 扩展后的边界
   */
  def getMBRAndCells(location: Long, xCell: Int, yCells: Int): (Double, Double, Double, Double) = {
    var i = 1
    var xmin = xLo
    var ymin = yLo
    var xmax = xHi
    var ymax = yHi
    var cs = 0L
    var leftCode = location
    // 解码位置编码，逐层确定象限
    while (i <= maxR) {
      if (cs != location) {
        val xCenter = (xmin + xmax) / 2.0
        val yCenter = (ymin + ymax) / 2.0
        val quadrant = leftCode / IS(i) // 当前层级的象限
        leftCode = leftCode - quadrant * IS(i)
        quadrant match {
          case 0 => // 左下象限
            cs += 1L
            xmax = xCenter
            ymax = yCenter
          case 1 => // 右下象限
            cs += 1L + 1L * IS(i)
            xmin = xCenter
            ymax = yCenter
          case 2 => // 左上象限
            cs += 1L + 2L * IS(i)
            xmax = xCenter
            ymin = yCenter
          case 3 => // 右上象限
            cs += 1L + 3L * IS(i)
            xmin = xCenter
            ymin = yCenter
        }
      }
      i += 1
    }
    val w = xmax - xmin // 网格宽度
    val h = ymax - ymin // 网格高度
    // 扩展到指定的网格数量
    (xmin, ymin, xmin + xCell * w, ymin + yCells * h)
  }

  /**
   * 获取指定位置编码对应的所有网格单元的 WKT 字符串
   *
   * @param location 四叉树位置编码
   * @param xCell    x 方向的网格数量
   * @param yCells   y 方向的网格数量
   * @return 网格单元的 WKT 字符串列表
   */
  def getCells(location: Long, xCell: Int, yCells: Int): util.ArrayList[String] = {
    val mbr = getMBRAndCells(location, xCell, yCells)
    val cellW = (mbr._3 - mbr._1) / alpha.toDouble // 单个网格宽度
    val cellH = (mbr._4 - mbr._2) / beta.toDouble // 单个网格高度
    val xmin = mbr._1
    val ymin = mbr._2
    val cells = new util.ArrayList[String]()
    // 生成所有网格单元的 WKT 字符串
    for (i <- 0 until alpha) {
      for (j <- 0 until beta) {
        val xMinCurrent = xmin + i * cellW
        val yMinCurrent = ymin + j * cellH
        cells.add(toPolyString(xMinCurrent, yMinCurrent, xMinCurrent + cellW, yMinCurrent + cellH))
      }
    }
    cells
  }

  /**
   * 将边界转换为 WKT POLYGON 字符串
   *
   * @param xmin 最小 x 坐标
   * @param ymin 最小 y 坐标
   * @param xmax 最大 x 坐标
   * @param ymax 最大 y 坐标
   * @return WKT 格式的 POLYGON 字符串
   */
  def toPolyString(xmin: Double, ymin: Double, xmax: Double, ymax: Double): String = {
    s"POLYGON (($xmin $ymin,$xmin $ymax,$xmax $ymax,$xmax $ymin,$xmin $ymin))"
  }

  /**
   * 计算几何体与网格的签名
   * 签名是一个位掩码，每一位表示对应的网格单元是否与几何体相交
   *
   * @param x        网格左下角 x 坐标
   * @param y        网格左下角 y 坐标
   * @param w        单个网格单元宽度
   * @param h        单个网格单元高度
   * @param geometry 几何体
   * @return 签名（位掩码），第 (i*beta+j) 位为 1 表示网格 (i,j) 与几何体相交
   */
  def signature(x: Double, y: Double, w: Double, h: Double, geometry: Geometry): Long = {
    var signature = 0L
    // 遍历所有网格单元
    for (i <- 0 until alpha) {
      for (j <- 0 until beta) {
        val minX = x + w * i
        val minY = y + h * j
        val env = new com.esri.core.geometry.Envelope(minX, minY, minX + w, minY + h)
        // 检查网格单元是否与几何体相交
        if (OperatorIntersects.local().execute(env, geometry, SpatialReference.create(4326), null)) {
          // 设置对应的位
          signature |= (1L << (i * beta + j).toLong)
        }
      }
    }
    signature
  }


  /**
   * 将坐标归一化到 [0,1] 范围
   * 将实际坐标转换为相对于空间边界的归一化坐标
   *
   * @param xmin    最小 x 坐标
   * @param ymin    最小 y 坐标
   * @param xmax    最大 x 坐标
   * @param ymax    最大 y 坐标
   * @param lenient 是否宽松模式（允许超出边界时自动裁剪）
   * @return 归一化后的坐标 (nxmin, nymin, nxmax, nymax)
   */
  def normalize(xmin: Double,
                ymin: Double,
                xmax: Double,
                ymax: Double,
                lenient: Boolean): (Double, Double, Double, Double) = {
    require(xmin <= xmax && ymin <= ymax, s"Bounds must be ordered: [$xmin $xmax] [$ymin $ymax]")

    try {
      // 严格模式：检查是否在边界内
      require(xmin >= xLo && xmax <= xHi && ymin >= yLo && ymax <= yHi,
        s"Values out of bounds ([$xLo $xHi] [$yLo $yHi]): [$xmin $xmax] [$ymin $ymax]")

      // 归一化到 [0,1]
      val nxmin = (xmin - xLo) / xSize
      val nymin = (ymin - yLo) / ySize
      val nxmax = (xmax - xLo) / xSize
      val nymax = (ymax - yLo) / ySize

      (nxmin, nymin, nxmax, nymax)
    } catch {
      case _: IllegalArgumentException if lenient =>
        // 宽松模式：自动裁剪到边界内
        val bxmin = clamp(xmin, xLo, xHi)
        val bymin = clamp(ymin, yLo, yHi)
        val bxmax = clamp(xmax, xLo, xHi)
        val bymax = clamp(ymax, yLo, yHi)

        val nxmin = (bxmin - xLo) / xSize
        val nymin = (bymin - yLo) / ySize
        val nxmax = (bxmax - xLo) / xSize
        val nymax = (bymax - yLo) / ySize

        (nxmin, nymin, nxmax, nymax)
    }
  }

  /**
   * 将值限制在指定范围内
   */
  private def clamp(value: Double, min: Double, max: Double): Double = {
    if (value < min) min
    else if (value > max) max
    else value
  }

  /**
   * 查询窗口，表示一个矩形查询区域
   */
  case class QueryWindow(xmin: Double, ymin: Double, xmax: Double, ymax: Double) {
    /**
     * 检查另一个矩形是否与查询窗口相交
     *
     * @param x1 矩形最小 x 坐标
     * @param y1 矩形最小 y 坐标
     * @param x2 矩形最大 x 坐标
     * @param y2 矩形最大 y 坐标
     * @return 是否相交
     */
    def insertion(x1: Double, y1: Double, x2: Double, y2: Double): Boolean = {
      xmax >= x1 && ymax >= y1 && xmin <= x2 && ymin <= y2
    }
  }

  /**
   * TShapeEE (Enlarged Element) 表示四叉树中的一个扩大元素
   * 扩展边界是 alpha * width 和 beta * height
   */
  case class TShapeEE(xmin: Double, ymin: Double, xmax: Double, ymax: Double, level: Int, elementCode: Long) {

    /**
     * 计算网格单元与轨迹的距离
     *
     * @param cells     网格单元的坐标数组
     * @param traj      轨迹
     * @param threshold 距离阈值
     * @return 距离
     */
    private def cellDistance(cells: Array[Coordinate], traj: Trajectory, threshold: Double): Double = {
      val factory = new GeometryFactory(new PrecisionModel(), 4326)
      val line = factory.createLinearRing(cells)
      val polygon = factory.createPolygon(line)
      polygon.distance(traj.getMultiPoint)
    }

    /**
     * 检查位置编码，找出与轨迹距离在阈值内的形状
     *
     * @param searchTraj 搜索轨迹
     * @param threshold  距离阈值
     * @param spoint     起点几何体（未使用）
     * @param epoint     终点几何体（未使用）
     * @return 索引范围列表
     */
    def checkPositionCode(searchTraj: Trajectory, threshold: Double, spoint: geom.Geometry, epoint: geom.Geometry): util.List[IndexRange] = {
      // 计算每个网格单元与轨迹的距离，标记异常单元
      var abnormalCell = 0L
      val results = new java.util.ArrayList[IndexRange](8)

      for (i <- 0 until alpha) {
        for (j <- 0 until beta) {
          val x = xmin + i * xLength
          val y = ymin + j * yLength
          val cell = Array(
            new Coordinate(x, y),
            new Coordinate(x, y + yLength),
            new Coordinate(x + xLength, y + yLength),
            new Coordinate(x + xLength, y),
            new Coordinate(x, y)
          )
          val dis = cellDistance(cell, searchTraj, threshold)
          // 如果距离超过阈值，标记为异常单元
          if (dis > threshold) {
            abnormalCell |= (1L << (i * beta + j).toLong)
          }
        }
      }

      // 找出不与异常单元相交的形状
      for (shapeCode <- shapes) {
        if ((shapeCode & abnormalCell) == 0) {
          // 形状不与任何异常单元相交，添加到结果中
          if (shapeMap != null && !shapeMap.isEmpty) {
            val optimizedCode = shapeMap.get(shapeCode)
            val index = optimizedCode.toLong | (elementCode << (alpha * beta).toLong)
            results.add(IndexRange(index, index, contained = false))
          } else {
            val index = shapeCode | (elementCode << (alpha * beta).toLong)
            results.add(IndexRange(index, index, contained = false))
          }
        }
      }
      results
    }

    /**
     * 检查是否需要检查该元素
     * 判断扩展后的元素边界是否包含查询边界
     *
     * @param boundaryEnv 查询边界
     * @param threshold   扩展阈值
     * @return 是否需要检查
     */
    def neededToCheck(boundaryEnv: geom.Envelope, threshold: Double): Boolean = {
      val enlElement = new geom.Envelope(xmin, exMax, ymin, eyMax)
      enlElement.expandBy(threshold)
      enlElement.contains(boundaryEnv)
    }

    // 元素尺寸
    val xLength: Double = xmax - xmin
    val yLength: Double = ymax - ymin

    // 扩展后的边界（考虑 alpha * beta 网格）
    private val exMax = xmin + xLength * alpha
    private val eyMax = ymin + yLength * beta

    val children = new java.util.ArrayList[TShapeEE](4)

    // 形状相关数据
    var shapes: List[Long] = _ // 形状编码列表
    var shapeMap: util.Map[Long, Int] = _ // 形状编码到优化编码的映射

    def setShapeMap(shapesMap: util.Map[Long, Int]): Unit = {
      if (null != shapesMap) {
        shapeMap = shapesMap
      }
    }

    /**
     * 在四叉树中搜索包含指定点的元素
     *
     * @param root 根节点
     * @param x    点的 x 坐标
     * @param y    点的 y 坐标
     * @param l    目标层级
     * @return 找到的元素
     */
    def search(root: TShapeEE, x: Double, y: Double, l: Int): TShapeEE = {
      var i = root.level
      var currentElement = root
      // 逐层向下搜索
      while (i < l) {
        val xCenter = (currentElement.xmin + currentElement.xmax) / 2.0
        val yCenter = (currentElement.ymin + currentElement.ymax) / 2.0
        // 根据点所在象限选择子节点
        (x < xCenter, y < yCenter) match {
          case (true, true) => // 左下
            currentElement = currentElement.getChildren.get(0)
          case (false, true) => // 右下
            currentElement = currentElement.getChildren.get(1)
          case (true, false) => // 左上
            currentElement = currentElement.getChildren.get(2)
          case (false, false) => // 右上
            currentElement = currentElement.getChildren.get(3)
        }
        i += 1
      }
      currentElement
    }

    /**
     * 检查元素是否与查询窗口相交
     *
     * @param window 查询窗口
     * @return 是否相交
     */
    def insertion(window: QueryWindow): Boolean = {
      window.xmax >= xmin && window.ymax >= ymin && window.xmin <= exMax && window.ymin <= eyMax
    }

    /**
     * 检查元素是否完全包含在查询窗口内
     *
     * @param window 查询窗口
     * @return 是否完全包含
     */
    def isContained(window: QueryWindow): Boolean = {
      window.xmin <= xmin && window.ymin <= ymin && window.xmax >= exMax && window.ymax >= eyMax
    }

    /**
     * 计算坐标对应的网格行/列索引
     *
     * @param x       坐标
     * @param xMin    最小坐标
     * @param xLength 网格长度
     * @return 网格索引
     */
    private def getLine(x: Double, xMin: Double, xLength: Double): Int = {
      Math.floor((x - xMin) / xLength).toInt
    }

    /**
     * 计算查询窗口在元素中的签名
     * 签名表示查询窗口覆盖了哪些网格单元
     *
     * @param window 查询窗口
     * @return 签名（位掩码）
     */
    def insertSignature(window: QueryWindow): Long = {
      var signature = 0L
      // 计算查询窗口覆盖的网格范围
      val xs = math.max(0, getLine(window.xmin, xmin, xLength))
      val xe = math.min(getLine(window.xmax, xmin, xLength) + 1, alpha)
      val ys = math.max(0, getLine(window.ymin, ymin, yLength))
      val ye = math.min(getLine(window.ymax, ymin, yLength) + 1, beta)

      // 标记覆盖的网格单元
      for (i <- xs until xe) {
        for (j <- ys until ye) {
          val minX = xmin + xLength * i
          val minY = ymin + yLength * j
          if (window.insertion(minX, minY, minX + xLength, minY + yLength)) {
            signature |= (1L << (i * beta + j).toLong)
          }
        }
      }
      signature
    }

    override def toString: String = {
      s"POLYGON (($xmin $ymin,$xmin $eyMax,$exMax $eyMax,$exMax $ymin,$xmin $ymin))"
    }

    /**
     * 将元素分裂为四个子元素
     */
    def split(): Unit = {
      if (children.isEmpty) {
        val xCenter = (xmax + xmin) / 2.0
        val yCenter = (ymax + ymin) / 2.0
        val intervalSize = (math.pow(4, maxR - level).toLong - 1L) / 3L
        // 创建四个子元素：左下、右下、左上、右上
        children.add(TShapeEE(xmin, ymin, xCenter, yCenter, level + 1, elementCode + 1L))
        children.add(TShapeEE(xCenter, ymin, xmax, yCenter, level + 1, elementCode + 1L + 1L * intervalSize))
        children.add(TShapeEE(xmin, yCenter, xCenter, ymax, level + 1, elementCode + 1L + 2L * intervalSize))
        children.add(TShapeEE(xCenter, yCenter, xmax, ymax, level + 1, elementCode + 1L + 3L * intervalSize))
      }
    }

    def setShapes(shape: List[Long]): Unit = {
      if (null == shapes) {
        shapes = shape
      }
    }

    def getShapes(indexMap: scala.collection.Map[Long, List[Long]]): List[Long] = {
      if (null != shapes) {
        return shapes
      }
      val indexSpaces = indexMap.get(elementCode)
      if (indexSpaces.isDefined) {
        shapes = indexSpaces.get
      }
      shapes
    }

    def getChildren: util.ArrayList[TShapeEE] = {
      if (children.isEmpty) {
        split()
      }
      children
    }

    //    def collectChildren(validQuadCodes: util.Set[lang.Long]): java.util.List[TShapeEE] = {
    //      val result = new java.util.ArrayList[TShapeEE]()
    //      val stack = new java.util.ArrayDeque[TShapeEE]()
    //
    //      stack.push(this)
    //
    //      while (!stack.isEmpty) {
    //        val quad = stack.pop()
    //        if (validQuadCodes.contains(quad.elementCode)) {
    //          result.add(quad)
    //        }
    //        //        result.add(quad)
    //        if (quad.level < maxR) {
    //          quad.split()
    //          quad.children.asScala.foreach(stack.push)
    //        }
    //      }
    //      result
    //    }

    def collectOrders(validQuadCodes: java.util.Set[java.lang.Long],
                      orderMap: java.util.Map[java.lang.Long, java.lang.Integer],
                      output: java.util.List[java.lang.Integer]): Unit = {

      val stack = new java.util.ArrayDeque[TShapeEE]()
      stack.push(this)

      while (!stack.isEmpty) {
        val quad = stack.pop()
        val code = quad.elementCode

        if (validQuadCodes.contains(code)) {
          output.add(orderMap.get(code))
        }

        if (quad.level < maxR) {
          quad.split()
          var i = 0
          while (i < 4) {
            val child = quad.children.get(i)
            if (child != null) stack.push(child)
            i += 1
          }
        }
      }
    }
  }

  /**
   * TShapeEE 表示 XZ 空间填充曲线中的一个扩大元素
   * 扩展边界是 2 * width 和 2 * height
   */
  case class XZ2EE(xmin: Double, ymin: Double, xmax: Double, ymax: Double, level: Int, elementCode: Long) {
    val xWidth: Double = xmax - xmin
    private val yHeight = ymax - ymin

    // 扩展后的边界（扩展 2 倍）
    private val exMax = xmin + xWidth * 2.0
    private val eyMax = ymin + yHeight * 2.0

    // 网格单元尺寸
    val xLength: Double = (xWidth * 2.0) / alpha.toDouble
    val yLength: Double = (yHeight * 2.0) / beta.toDouble

    val children = new java.util.ArrayList[XZ2EE](4)

    // 形状相关数据
    var shapes: List[Long] = _ // 形状编码列表
    var shapeMap: util.Map[Long, Int] = _ // 形状编码到优化编码的映射

    def setShapeMap(shapesMap: util.Map[Long, Int]): Unit = {
      if (null != shapesMap) {
        shapeMap = shapesMap
      }
    }

    /**
     * 检查元素是否与查询窗口相交
     */
    def insertion(window: QueryWindow): Boolean = {
      window.xmax >= xmin && window.ymax >= ymin && window.xmin <= exMax && window.ymin <= eyMax
    }

    /**
     * 检查元素是否完全包含在查询窗口内
     */
    def isContained(window: QueryWindow): Boolean = {
      window.xmin <= xmin && window.ymin <= ymin && window.xmax >= exMax && window.ymax >= eyMax
    }

    /**
     * 计算坐标对应的网格行/列索引
     */
    private def getLine(x: Double, xMin: Double, xLength: Double): Int = {
      ((x - xMin) / xLength).toInt
    }

    /**
     * 计算查询窗口在元素中的签名
     */
    def insertSignature(window: QueryWindow): Long = {
      var signature = 0L
      // 计算查询窗口覆盖的网格范围
      val xs = getLine(window.xmin, xmin, xLength)
      val xe = math.min(getLine(window.xmax, xmin, xLength) + 1, alpha)
      val ys = getLine(window.ymin, ymin, yLength)
      val ye = math.min(getLine(window.ymax, ymin, yLength) + 1, beta)

      // 标记覆盖的网格单元
      for (i <- xs until xe) {
        for (j <- ys until ye) {
          val minX = xmin + xLength * i
          val minY = ymin + yLength * j
          if (window.insertion(minX, minY, minX + xLength, minY + yLength)) {
            signature |= (1L << (i * beta + j).toLong)
          }
        }
      }
      signature
    }

    override def toString: String = {
      s"POLYGON (($xmin $ymin,$xmin $eyMax,$exMax $eyMax,$exMax $ymin,$xmin $ymin))"
    }

    /**
     * 将元素分裂为四个子元素
     */
    def split(): Unit = {
      if (children.isEmpty) {
        val xCenter = (xmax + xmin) / 2.0
        val yCenter = (ymax + ymin) / 2.0
        //        val intervalSize = (math.pow(4, maxR - level).toLong - 1L) / 3L
        val intervalSize = IS(level + 1)
        // 创建四个子元素：左下、右下、左上、右上
        children.add(XZ2EE(xmin, ymin, xCenter, yCenter, level + 1, elementCode + 1L))
        children.add(XZ2EE(xCenter, ymin, xmax, yCenter, level + 1, elementCode + 1L + 1L * intervalSize))
        children.add(XZ2EE(xmin, yCenter, xCenter, ymax, level + 1, elementCode + 1L + 2L * intervalSize))
        children.add(XZ2EE(xCenter, yCenter, xmax, ymax, level + 1, elementCode + 1L + 3L * intervalSize))
      }
    }

    def setShapes(shape: List[Long]): Unit = {
      if (null == shapes) {
        shapes = shape
      }
    }

    def getShapes(indexMap: scala.collection.Map[Long, List[Long]]): List[Long] = {
      if (null != shapes) {
        return shapes
      }
      val indexSpaces = indexMap.get(elementCode)
      if (indexSpaces.isDefined) {
        shapes = indexSpaces.get
      }
      shapes
    }
  }
}

object XZSFC {
  val DefaultPrecision: Short = 12

  val LogPointFive: Double = math.log(0.5)

  //  private def intersectCellNumber(v1: Long, v2: Long): Int = {
  //    val v = v1 & v2
  //    NumberUtil.bit1(v)
  //  }
  //
  //  def encodeShapes(shapes: java.util.List[Object]): = {
  //    if (shapes != null) {
  //      var coverage = Array.ofDim[Int](shapes.size, shapes.size)
  //      for (i <- 0 until shapes.size - 1) {
  //        coverage(i)(i) = 0
  //        for (j <- i + 1 until shapes.size) {
  //          val number = intersectCellNumber(shapes.get(i).asInstanceOf[Long], shapes.get(j).asInstanceOf[Long])
  //          coverage(i)(j) = number
  //          coverage(j)(i) = number
  //        }
  //      }
  //      val index = TSPGreedy.tspGreedy(coverage)
  //      index
  //    }
  //  }
}