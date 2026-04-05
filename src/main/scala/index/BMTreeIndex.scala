package index

import com.esri.core.geometry._
import org.locationtech.sfcurve.IndexRange

import scala.annotation.tailrec
import scala.io.Source

/**
 * BMTree节点
 * 
 * @param nodeId      节点ID
 * @param parentId    父节点ID（-1表示根节点）
 * @param depth       节点深度
 * @param chosenBit   选择的bit维度（0=x, 1=y, -1表示叶子节点）
 * @param leftChild   左子节点ID（-1表示无）
 * @param rightChild  右子节点ID（-1表示无）
 * @param child       非分割子节点ID（-1表示无）
 */
case class BMTreeNode(
  nodeId: Int,
  parentId: Int,
  depth: Int,
  chosenBit: Int,
  leftChild: Int,
  rightChild: Int,
  child: Int
)

/**
 * BMTree (Binary Multi-dimensional Tree) 索引实现
 * 
 * BMTree是一种自适应的空间填充曲线，通过MCTS学习得到最优的bit选择策略
 * 
 * @param maxR              最大分辨率（兼容XZSFC基类）
 * @param xBounds           x轴边界 (min, max)
 * @param yBounds           y轴边界 (min, max)
 * @param bmtreeConfigPath  BMTree配置文件路径（best_tree.txt）
 * @param bitLength         每个维度的bit长度数组，例如 Array(20, 20)
 */
class BMTreeIndex(
  maxR: Short,
  xBounds: (Double, Double),
  yBounds: (Double, Double),
  bmtreeConfigPath: String,
  bitLength: Array[Int]
) extends XZSFC(maxR, xBounds, yBounds, 2, 2) with Serializable {

  // 空间边界
  private val xLo = xBounds._1
  private val xHi = xBounds._2
  private val yLo = yBounds._1
  private val yHi = yBounds._2

  // 空间尺寸
  override val xSize: Double = xHi - xLo
  override val ySize: Double = yHi - yLo

  // 网格大小（基于第一个维度的bit长度）
  private val gridSize: Int = 1 << bitLength(0)

  // BMTree结构
  private val nodes: Array[BMTreeNode] = loadBMTree(bmtreeConfigPath)
  private val rootNode: BMTreeNode = nodes(0)

  println(s"[BMTreeIndex] 加载BMTree成功: ${nodes.length} 个节点, 根节点深度=${rootNode.depth}")

  /**
   * 从配置文件加载BMTree结构
   * 
   * 文件格式：
   * 第1行：维度数 bit_length[0] bit_length[1] ...
   * 第2行：max_depth
   * 第3行起：node_id parent_id depth chosen_bit left_child right_child child
   */
  private def loadBMTree(path: String): Array[BMTreeNode] = {
    try {
      // 尝试从类路径加载
      val stream = getClass.getClassLoader.getResourceAsStream(path)
      if (stream == null) {
        throw new IllegalArgumentException(s"BMTree配置文件未找到: $path")
      }

      val lines = Source.fromInputStream(stream).getLines().toList
      stream.close()

      if (lines.length < 3) {
        throw new IllegalArgumentException(s"BMTree配置文件格式错误: 行数不足")
      }

      // 解析第1行：维度数和bit长度
      val header = lines.head.trim.split("\\s+").map(_.toInt)
      val dim = header(0)
      val fileBitLength = header.slice(1, dim + 1)

      // 验证bit长度一致性
      if (!fileBitLength.sameElements(bitLength)) {
        throw new IllegalArgumentException(
          s"BMTree配置文件的bit_length ${fileBitLength.mkString(",")} " +
          s"与配置不匹配 ${bitLength.mkString(",")}"
        )
      }

      // 解析第2行：max_depth
      val maxDepth = lines(1).trim.toInt
      println(s"[BMTreeIndex] BMTree max_depth=$maxDepth, bit_length=${bitLength.mkString(",")}")

      // 解析节点信息
      val nodeLines = lines.drop(2).filter(_.trim.nonEmpty)
      val parsedNodes = nodeLines.map { line =>
        val parts = line.trim.split("\\s+").map(_.toInt)
        if (parts.length < 7) {
          throw new IllegalArgumentException(s"节点行格式错误: $line")
        }
        BMTreeNode(parts(0), parts(1), parts(2), parts(3), parts(4), parts(5), parts(6))
      }.toArray

      println(s"[BMTreeIndex] 成功加载 ${parsedNodes.length} 个节点")
      parsedNodes

    } catch {
      case e: Exception =>
        throw new RuntimeException(s"加载BMTree配置文件失败: $path", e)
    }
  }

  /**
   * 将实际坐标归一化到网格坐标
   */
  @inline
  private def normalizeToGrid(value: Double, min: Double, max: Double): Int = {
    val normalized = (value - min) / (max - min)
    math.min((normalized * gridSize).toInt, gridSize - 1)
  }

  /**
   * 将整数转换为二进制bit数组（LSB first）
   * 
   * @param value  整数值
   * @param length bit长度
   * @return bit数组，索引0是最低位
   */
  private def toBinary(value: Int, length: Int): Array[Int] = {
    val bits = new Array[Int](length)
    var i = 0
    while (i < length) {
      bits(i) = (value >> i) & 1
      i += 1
    }
    bits
  }

  /**
   * 计算从根节点到当前节点路径上，指定维度已使用的bit数
   * 
   * @param node 当前节点
   * @param dim  维度（0=x, 1=y）
   * @return 已使用的bit数
   */
  private def countBitsUsed(node: BMTreeNode, dim: Int): Int = {
    var count = 0
    var currentId = node.nodeId

    // 从当前节点向上遍历到根节点
    while (currentId != -1 && currentId < nodes.length) {
      val n = nodes(currentId)
      if (n.chosenBit == dim) {
        count += 1
      }
      currentId = n.parentId
    }
    count
  }

  /**
   * 使用Z-order填充剩余bit
   * 当到达叶子节点或非完全分割节点时，使用Z-order填充剩余的bit
   * 
   * @param xBits       x维度的bit数组（LSB first存储）
   * @param yBits       y维度的bit数组（LSB first存储）
   * @param value       当前累积的SFC值
   * @param node        当前节点
   * @return 最终的SFC值
   */
  private def fillWithZOrder(
    xBits: Array[Int],
    yBits: Array[Int],
    value: Long,
    node: BMTreeNode
  ): Long = {
    var result = value
    val xUsed = countBitsUsed(node, 0)
    val yUsed = countBitsUsed(node, 1)

    // 从MSB（高位）开始填充，保证单调性
    var xi = bitLength(0) - 1 - xUsed
    var yi = bitLength(1) - 1 - yUsed

    // Z-order交替填充剩余bit（从高位到低位）
    while (xi >= 0 || yi >= 0) {
      if (xi >= 0) {
        result = (result << 1) | xBits(xi)
        xi -= 1
      }
      if (yi >= 0) {
        result = (result << 1) | yBits(yi)
        yi -= 1
      }
    }
    result
  }

  /**
   * 递归计算SFC值（遍历BMTree）
   * 
   * @param xBits       x维度的bit数组（LSB first存储）
   * @param yBits       y维度的bit数组（LSB first存储）
   * @param node        当前节点
   * @param parentValue 父节点累积的SFC值
   * @return 最终的SFC值
   */
  @tailrec
  private def computeSFC(
    xBits: Array[Int],
    yBits: Array[Int],
    node: BMTreeNode,
    parentValue: Long
  ): Long = {
    // 叶子节点：使用Z-order填充剩余bit
    if (node.chosenBit == -1) {
      return fillWithZOrder(xBits, yBits, parentValue, node)
    }

    // 获取当前节点选择的bit
    val chosenDim = node.chosenBit  // 0=x, 1=y
    val bitsUsed = countBitsUsed(node, chosenDim)
    
    // 从MSB（高位）开始访问，保证单调性
    val bitIndex = if (chosenDim == 0) {
      bitLength(0) - 1 - bitsUsed
    } else {
      bitLength(1) - 1 - bitsUsed
    }
    
    // 获取对应维度的bit值
    val bitValue = if (chosenDim == 0) {
      if (bitIndex >= 0 && bitIndex < xBits.length) xBits(bitIndex) else 0
    } else {
      if (bitIndex >= 0 && bitIndex < yBits.length) yBits(bitIndex) else 0
    }

    // 累积SFC值：左移1位，加上当前bit
    val newValue = (parentValue << 1) | bitValue

    // 选择子节点
    val nextNodeId = if (bitValue == 0) {
      // bit=0，选择左子节点或非分割子节点
      if (node.leftChild != -1) node.leftChild
      else if (node.child != -1) node.child
      else return fillWithZOrder(xBits, yBits, newValue, node)
    } else {
      // bit=1，选择右子节点或非分割子节点
      if (node.rightChild != -1) node.rightChild
      else if (node.child != -1) node.child
      else return fillWithZOrder(xBits, yBits, newValue, node)
    }

    // 检查子节点是否存在
    if (nextNodeId < 0 || nextNodeId >= nodes.length) {
      return fillWithZOrder(xBits, yBits, newValue, node)
    }

    val nextNode = nodes(nextNodeId)

    // 递归计算
    computeSFC(xBits, yBits, nextNode, newValue)
  }

  /**
   * 为几何体创建BMTree索引
   */
  override def index(geometry: Geometry, lenient: Boolean = false): (Int, Long, Long) = {
    val mbr = new Envelope2D()
    geometry.queryEnvelope2D(mbr)

    val minX = normalizeToGrid(mbr.xmin, xLo, xHi)
    val minY = normalizeToGrid(mbr.ymin, yLo, yHi)
    val maxX = normalizeToGrid(mbr.xmax, xLo, xHi)
    val maxY = normalizeToGrid(mbr.ymax, yLo, yHi)

    val minXBits = toBinary(minX, bitLength(0))
    val minYBits = toBinary(minY, bitLength(1))
    val maxXBits = toBinary(maxX, bitLength(0))
    val maxYBits = toBinary(maxY, bitLength(1))

    val minSFC = computeSFC(minXBits, minYBits, rootNode, 0L)
    val maxSFC = computeSFC(maxXBits, maxYBits, rootNode, 0L)

    (0, minSFC, maxSFC)
  }

  /**
   * 基础范围查询（从0开始，保证不漏检）
   */
  def ranges(
    lng1: Double,
    lat1: Double,
    lng2: Double,
    lat2: Double
  ): java.util.List[IndexRange] = {
    // 归一化到网格坐标
    val qMinX = normalizeToGrid(lng1, xLo, xHi)
    val qMinY = normalizeToGrid(lat1, yLo, yHi)
    val qMaxX = normalizeToGrid(lng2, xLo, xHi)
    val qMaxY = normalizeToGrid(lat2, yLo, yHi)

    // 计算4个角的SFC值
    val sfc_LL = computeSFC(toBinary(qMinX, bitLength(0)), toBinary(qMinY, bitLength(1)), rootNode, 0L)  // 左下
    val sfc_LR = computeSFC(toBinary(qMaxX, bitLength(0)), toBinary(qMinY, bitLength(1)), rootNode, 0L)  // 右下
    val sfc_UL = computeSFC(toBinary(qMinX, bitLength(0)), toBinary(qMaxY, bitLength(1)), rootNode, 0L)  // 左上
    val sfc_UR = computeSFC(toBinary(qMaxX, bitLength(0)), toBinary(qMaxY, bitLength(1)), rootNode, 0L)  // 右上

    val maxSFC = math.max(math.max(sfc_LL, sfc_LR), math.max(sfc_UL, sfc_UR))

    val ranges = new java.util.ArrayList[IndexRange]()
    ranges.add(IndexRange(0, maxSFC, contained = false))
    ranges
  }
}

object BMTreeIndex extends Serializable {
  private val cache = new java.util.concurrent.ConcurrentHashMap[
    (String, Short, (Double, Double), (Double, Double), String, String),
    BMTreeIndex
  ]()

  def apply(
    g: Short,
    xBounds: (Double, Double),
    yBounds: (Double, Double),
    bmtreeConfigPath: String,
    bitLength: Array[Int]
  ): BMTreeIndex = {
    val bitLengthStr = bitLength.mkString(",")
    val key = ("", g, xBounds, yBounds, bmtreeConfigPath, bitLengthStr)
    var sfc = cache.get(key)
    if (sfc == null) {
      sfc = new BMTreeIndex(g, xBounds, yBounds, bmtreeConfigPath, bitLength)
      cache.put(key, sfc)
    }
    sfc
  }

  def apply(
    table: String,
    g: Short,
    xBounds: (Double, Double),
    yBounds: (Double, Double),
    bmtreeConfigPath: String,
    bitLength: Array[Int]
  ): BMTreeIndex = {
    val bitLengthStr = bitLength.mkString(",")
    val key = (table, g, xBounds, yBounds, bmtreeConfigPath, bitLengthStr)
    var sfc = cache.get(key)
    if (sfc == null) {
      sfc = new BMTreeIndex(g, xBounds, yBounds, bmtreeConfigPath, bitLength)
      cache.put(key, sfc)
    }
    sfc
  }
}
