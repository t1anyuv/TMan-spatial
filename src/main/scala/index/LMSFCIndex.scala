package index

import com.esri.core.geometry._
import org.locationtech.sfcurve.IndexRange

class LMSFCIndex(
                  maxR: Short,
                  xBounds: (Double, Double),
                  yBounds: (Double, Double),
                  thetaConfigStr: String
                ) extends XZSFC(maxR, xBounds, yBounds, 2, 2) with Serializable {

  private val xLo = xBounds._1
  private val xHi = xBounds._2
  private val yLo = yBounds._1
  private val yHi = yBounds._2

  override val xSize: Double = xHi - xLo
  override val ySize: Double = yHi - yLo

  private val thetaConfig: Array[Int] = thetaConfigStr.split(",").map(_.trim.toInt)
  private val bitNum = thetaConfig.length / 2
  validateThetaConfig(thetaConfig)

  private val gridSize: Int = 1 << bitNum
  private val thetaX: Array[Long] = thetaConfig.slice(0, bitNum).map(i => 1L << i)
  private val thetaY: Array[Long] = thetaConfig.slice(bitNum, thetaConfig.length).map(i => 1L << i)

  def validateThetaConfig(config: Array[Int]): Unit = {
    require(config.length == bitNum * 2, s"theta config length must be ${bitNum * 2}")
    require(config.forall(i => i >= 0 && i < 63), "theta bit offsets must be between 0 and 62")
    require(config.distinct.length == config.length, "theta bit offsets must be unique")

    val thetaXIdx = config.slice(0, bitNum)
    val thetaYIdx = config.slice(bitNum, config.length)

    for (i <- 0 until bitNum - 1) {
      require(thetaXIdx(i) < thetaXIdx(i + 1), s"thetaX must be strictly increasing at index $i")
      require(thetaYIdx(i) < thetaYIdx(i + 1), s"thetaY must be strictly increasing at index $i")
    }
  }

  @inline
  private def normalizeToGrid(value: Double, min: Double, max: Double): Int = {
    val normalized = (value - min) / (max - min)
    math.min((normalized * gridSize).toInt, gridSize - 1)
  }

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

    val maxSFC = calcSFCPosition(qMaxX, qMaxY)

    val ranges = new java.util.ArrayList[IndexRange]()
    ranges.add(IndexRange(0, maxSFC, contained = false))
    ranges
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
