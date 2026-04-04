package filter;

import com.esri.core.geometry.*;
import com.google.protobuf.InvalidProtocolBufferException;
import config.TableConfig;
import constans.IndexEnum;
import index.LETILocSIndex;
import index.LocSIndex;
import index.XZStarIndex;
import index.XZLocSIndex;
import preprocess.compress.IIntegerCompress;
import lombok.Getter;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.util.Bytes;
import org.locationtech.sfcurve.IndexRange;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;
import redis.clients.jedis.resps.Tuple;
import scala.Tuple2;
import utils.RedisPoolManager;

import java.util.ArrayList;
import java.util.List;

import static client.Constants.GEOM_X;
import static client.Constants.GEOM_Y;


public class SpatialFilter extends FilterBase {
    protected final String geom;
    protected final String operationType;
    protected final String compressType;
    protected final Geometry geometry;
    protected boolean foundLatitude = false;
    protected boolean foundLongitude = false;
    protected boolean checked = false;
    protected int[] latitudes;
    protected int[] longitudes;
    protected boolean filterRow = false;

    @Getter
    protected IndexEnum.INDEX_TYPE indexType = IndexEnum.INDEX_TYPE.SPATIAL;
    protected OperatorImportFromWkt importerWKT = (OperatorImportFromWkt) OperatorFactoryLocal.getInstance().getOperator(Operator.Type.ImportFromWkt);

    public SpatialFilter(String geom, String operationType, String compressType) {
        this.geom = geom;
        this.operationType = operationType;
        this.compressType = compressType;
        OperatorImportFromWkt importerWKT = (OperatorImportFromWkt) OperatorFactoryLocal.getInstance().getOperator(Operator.Type.ImportFromWkt);
        this.geometry = importerWKT.execute(0, Geometry.Type.Polygon, geom, null);
    }

    public SpatialFilter(String geom, String compressType) {
        this(geom, null, compressType);
    }

    public static Filter parseFrom(final byte[] pbBytes) {
        tman.filters.generated.SpatialFilter.SpatialFilterMesg mesg;
        try {
            mesg = tman.filters.generated.SpatialFilter.SpatialFilterMesg.parseFrom(pbBytes);
        } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException(e);
        }
        return new SpatialFilter(mesg.getGeomWKT(), mesg.getType(), mesg.getCompressType());
    }

    /**
     * 运行时依赖自检（fail-fast）：
     * 提前验证 protobuf 生成类是否可用，避免在 RegionServer 查询阶段才出现
     * "Unresolved compilation problems / tman cannot be resolved" 这类延迟报错。
     */
    public static void validateRuntimeDependencies() {
        try {
            Class<?> mesgClass = Class.forName("tman.filters.generated.SpatialFilter$SpatialFilterMesg");
            mesgClass.getMethod("parseFrom", byte[].class);
            mesgClass.getMethod("newBuilder");
        } catch (Throwable t) {
            String message =
                    "SpatialFilter protobuf runtime check failed: missing or broken generated class "
                            + "'tman.filters.generated.SpatialFilter$SpatialFilterMesg'. "
                            + "Please rebuild generated sources and runtime classes (e.g. run 'mvn -DskipTests compile') "
                            + "and ensure runtime classpath points to the latest target/classes.";
            throw new IllegalStateException(message, t);
        }
    }

    /**
     * 子类可以重写此方法以添加额外的过滤逻辑（如 DP 检查）
     * 在调用父类方法之前执行
     */
    protected ReturnCode filterCellBefore(Cell c) {
        return null; // null 表示继续执行默认逻辑
    }

    @Override
    public ReturnCode filterCell(Cell c) {
        // 允许子类在默认逻辑之前添加额外的检查
        ReturnCode result = filterCellBefore(c);
        if (result != null) {
            return result;
        }

        if (this.filterRow || checked) {
            return checked ? ReturnCode.INCLUDE : ReturnCode.NEXT_ROW;
        }

        String qualifier = Bytes.toString(CellUtil.cloneQualifier(c));

        if (qualifier.equals(GEOM_X)) {
            foundLatitude = true;
            latitudes = IIntegerCompress.getIntegerCompress(compressType).decoding(CellUtil.cloneValue(c));
        } else if (qualifier.equals(GEOM_Y)) {
            foundLongitude = true;
            longitudes = IIntegerCompress.getIntegerCompress(compressType).decoding(CellUtil.cloneValue(c));
        }

        if (foundLatitude && foundLongitude) {
            int size = latitudes.length;
            MultiPoint multiPoint = new MultiPoint();
            multiPoint.resize(size);
            for (int i = 0; i < size; i++) {
                multiPoint.setPoint(i, new Point(latitudes[i] / Math.pow(10, 6), longitudes[i] / Math.pow(10, 6)));
            }
            this.filterRow = !OperatorIntersects.local().execute(geometry, multiPoint, SpatialReference.create(4326), null);
            this.checked = true;

            if (this.filterRow) {
                this.foundLatitude = false;
                this.foundLongitude = false;
                this.checked = false;
                return ReturnCode.NEXT_ROW;
            }
        }
        return ReturnCode.INCLUDE;
    }

    /**
     * 获取空间查询的索引范围
     *
     * @param tableName 表名
     * @param config    表配置
     * @return 索引范围列表
     */
    public List<IndexRange> getRanges(String tableName, TableConfig config) {
        Envelope2D envelope = new Envelope2D();
        this.geometry.queryEnvelope2D(envelope);

        if (config.getSpatialIndexKind() == TableConfig.SpatialIndexKind.LETI
                && config.getPrimary().equals(IndexEnum.INDEX_TYPE.SPATIAL)) {
            return getRangesWithLETI(tableName, config, envelope);
        }

        return getRangesWithLocS(tableName, config, envelope);
    }

    /**
     * 使用 LETI 索引获取范围
     */
    private List<IndexRange> getRangesWithLETI(String tableName, TableConfig config, Envelope2D envelope) {
        Envelope letiBoundary = config.getLetiOrderBoundary();
        if (letiBoundary != null && !intersects(envelope, letiBoundary)) {
            return new ArrayList<>();
        }
        LETILocSIndex letiLocSIndex;
        String orderingPath = config.getOrderDefinitionPath();
        if (null != config.getEnvelope()) {
            letiLocSIndex = LETILocSIndex.apply(
                    tableName,
                    (short) config.getResolution(),
                    new Tuple2<>(config.getEnvelope().getXMin(), config.getEnvelope().getXMax()),
                    new Tuple2<>(config.getEnvelope().getYMin(), config.getEnvelope().getYMax()),
                    config.getAlpha(),
                    config.getBeta(),
                    config.isAdaptivePartition(),
                    orderingPath
            );
        } else {
            letiLocSIndex = LETILocSIndex.apply(
                    tableName,
                    (short) config.getResolution(),
                    config.getAlpha(),
                    config.getBeta(),
                    config.isAdaptivePartition(),
                    orderingPath
            );
        }
        try (Jedis jedis = RedisPoolManager.getResource(config.getRedisHost())) {
            if (Boolean.getBoolean("tman.leti.useTShapeFlowRanges")) {
                return letiLocSIndex.rangesWithTShapeFlow(
                        envelope.xmin,
                        envelope.ymin,
                        envelope.xmax,
                        envelope.ymax,
                        jedis,
                        tableName
                );
            }
            return letiLocSIndex.ranges(envelope.xmin, envelope.ymin, envelope.xmax, envelope.ymax, jedis, tableName);
        }
    }

    private boolean intersects(Envelope2D query, Envelope boundary) {
        return !(query.xmax < boundary.getXMin()
                || query.xmin > boundary.getXMax()
                || query.ymax < boundary.getYMin()
                || query.ymin > boundary.getYMax());
    }

    /**
     * 使用 LocS 索引获取范围
     */
    private List<IndexRange> getRangesWithLocS(String tableName, TableConfig config, Envelope2D envelope) {
        LocSIndex locSIndex;
        if (null != config.getEnvelope()) {
            locSIndex = LocSIndex.apply((short) config.getResolution(), new Tuple2<>(config.getEnvelope().getXMin(), config.getEnvelope().getXMax()), new Tuple2<>(config.getEnvelope().getYMin(), config.getEnvelope().getYMax()), config.getAlpha(), config.getBeta());
        } else {
            locSIndex = LocSIndex.apply((short) config.getResolution(), config.getAlpha(), config.getBeta());
        }
        // 不使用 redis
        try (Jedis jedis = new Jedis(config.getRedisHost(), 6379)) {
            if (config.isTspEncoding()) {
                return locSIndex.rangesThread(
                        envelope.xmin,
                        envelope.ymin,
                        envelope.xmax,
                        envelope.ymax,
                        jedis,
                        tableName,
                        true);
            }
            return locSIndex.ranges(envelope.xmin, envelope.ymin, envelope.xmax, envelope.ymax, jedis, tableName);
        }
    }

    /**
     * 获取 XZ 编码空间查询的索引范围
     *
     * @param tableName 表名
     * @param config    表配置
     * @return 索引范围列表
     */
    public List<IndexRange> getXZRanges(String tableName, TableConfig config) {
        Envelope2D envelope = new Envelope2D();
        this.geometry.queryEnvelope2D(envelope);

        if (config.getSpatialIndexKind() == TableConfig.SpatialIndexKind.LETI
                && config.getPrimary().equals(IndexEnum.INDEX_TYPE.SPATIAL)) {
            return getRangesWithLETI(tableName, config, envelope);
        }

        return getXZRangesWithXZLocS(tableName, config, envelope);
    }

    /**
     * 使用 XZLocS 索引获取范围
     */
    private List<IndexRange> getXZRangesWithXZLocS(String tableName, TableConfig config, Envelope2D envelope) {
        if (config.getSpatialIndexKind() == TableConfig.SpatialIndexKind.XZ_STAR) {
            return getXZStarRanges(tableName, config, envelope);
        }

        XZLocSIndex xzLocSIndex;
        if (null != config.getEnvelope()) {
            xzLocSIndex = XZLocSIndex.apply((short) config.getResolution(), new Tuple2<>(config.getEnvelope().getXMin(), config.getEnvelope().getXMax()), new Tuple2<>(config.getEnvelope().getYMin(), config.getEnvelope().getYMax()), config.getAlpha(), config.getBeta());
        } else {
            xzLocSIndex = XZLocSIndex.apply((short) config.getResolution(), config.getAlpha(), config.getBeta());
        }
        try (Jedis jedis = new Jedis(config.getRedisHost(), 6379)) {
            if (config.isTspEncoding()) {
                return xzLocSIndex.rangesThread(envelope.xmin, envelope.ymin, envelope.xmax, envelope.ymax, jedis, tableName, true);
            }
            return xzLocSIndex.ranges(envelope.xmin, envelope.ymin, envelope.xmax, envelope.ymax, jedis, tableName);
        }
    }

    private List<IndexRange> getXZStarRanges(String tableName, TableConfig config, Envelope2D envelope) {
        XZStarIndex xzStarIndex;
        if (null != config.getEnvelope()) {
            xzStarIndex = XZStarIndex.apply(
                    (short) config.getResolution(),
                    new Tuple2<>(config.getEnvelope().getXMin(), config.getEnvelope().getXMax()),
                    new Tuple2<>(config.getEnvelope().getYMin(), config.getEnvelope().getYMax())
            );
        } else {
            xzStarIndex = XZStarIndex.apply(
                    (short) config.getResolution(),
                    new Tuple2<>(-180.0, 180.0),
                    new Tuple2<>(-90.0, 90.0)
            );
        }
        return xzStarIndex.ranges(envelope.xmin, envelope.ymin, envelope.xmax, envelope.ymax);
    }

    public long getCost(String tableName, TableConfig config) {
        List<IndexRange> indexRanges;
        switch (config.getSpatialIndexKind()) {
            case XZPlus:
            case XZ_STAR:
                indexRanges = getXZRanges(tableName, config);
                break;
            case LETI:
            case TShape:
            default:
                indexRanges = getRanges(tableName, config);
                break;
        }

        long totalSize = 0;
        try (Jedis jedis = config.getSpatialIndexKind() == TableConfig.SpatialIndexKind.LETI
                ? RedisPoolManager.getResource(config.getRedisHost())
                : new Jedis(config.getRedisHost(), 6379)) {
            Pipeline pipelined = jedis.pipelined();
            List<Response<List<Tuple>>> responses = new ArrayList<>();

            for (IndexRange indexRange : indexRanges) {
                try {
                    responses.add(pipelined.zrangeByScoreWithScores(tableName, indexRange.lower(), indexRange.upper()));
                } catch (Exception e) {
                    System.err.println("[SpatialFilter] Error querying index range: " + e.getMessage());
                }
            }
            pipelined.sync();

            for (Response<List<Tuple>> response : responses) {
                for (Tuple tuple : response.get()) {
                    byte[] indexedSize = new byte[4];
                    System.arraycopy(tuple.getBinaryElement(), 0, indexedSize, 0, 4);
                    totalSize += Bytes.toInt(indexedSize);
                }
            }
        } catch (Exception e) {
            System.err.println("[SpatialFilter] Error calculating cost: " + e.getMessage());
        }
        return totalSize;
    }

    @Override
    public boolean filterRow() {
        return this.filterRow;
    }

    @Override
    public void reset() {
        this.foundLatitude = false;
        this.foundLongitude = false;
        this.filterRow = false;
        this.checked = false;
    }

    @Override
    public byte[] toByteArray() {
        tman.filters.generated.SpatialFilter.SpatialFilterMesg.Builder builder = tman.filters.generated.SpatialFilter.SpatialFilterMesg.newBuilder();
        builder.setCompressType(compressType);
        builder.setGeomWKT(geom);
        if (null != operationType) {
            builder.setType(operationType);
        }
        return builder.build().toByteArray();
    }

}

