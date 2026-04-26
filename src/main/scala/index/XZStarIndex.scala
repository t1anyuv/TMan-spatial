package index

import org.locationtech.jts.geom.Geometry
import org.locationtech.sfcurve.IndexRange

import scala.collection.JavaConverters._

/**
 * XZ_STAR 索引适配层：
 * - 编码：用 XZStarSFC.index(geometry) 直接生成最终 rowkey 的“index(long)”
 * - 查询：用 XZStarSFC.ranges(lng1,lat1,lng2,lat2) 生成 IndexRange，供 QueryPlanner/rangesToRowkey 扫描
 *
 * XZ_STAR 固定使用 2*2，且不参与 tspEncoding 分支。
 */
class XZStarIndex(
    val g: Short,
    val xBounds: (Double, Double),
    val yBounds: (Double, Double)
) extends Serializable {

  private val core: XZStarSFC = XZStarSFC(g, xBounds, yBounds, beta = 1)

  def index(geometry: Geometry, lenient: Boolean = false): Long = {
    core.index(geometry, lenient)
  }

  def ranges(lng1: Double, lat1: Double, lng2: Double, lat2: Double): java.util.List[IndexRange] = {
    core.ranges(lng1, lat1, lng2, lat2)
  }

  def ranges(lng1: Double, lat1: Double, lng2: Double, lat2: Double, jedis: redis.clients.jedis.Jedis, indexTable: String): java.util.List[IndexRange] = {
    ranges(lng1, lat1, lng2, lat2)
  }

  def rangesThread(
      lng1: Double,
      lat1: Double,
      lng2: Double,
      lat2: Double,
      jedis: redis.clients.jedis.Jedis,
      indexTable: String,
      tspEncoding: Boolean
  ): java.util.List[IndexRange] = {
    ranges(lng1, lat1, lng2, lat2)
  }
}

object XZStarIndex extends Serializable {
  def apply(g: Short, xBounds: (Double, Double), yBounds: (Double, Double)): XZStarIndex = {
    new XZStarIndex(g, xBounds, yBounds)
  }
}

