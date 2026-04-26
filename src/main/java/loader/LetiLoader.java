package loader;

import com.esri.core.geometry.*;
import config.TableConfig;
import utils.ByteArraysWrapper;
import utils.LetiOrderResolver;
import utils.LSFCReader;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.storage.StorageLevel;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import scala.Tuple2;
import scala.Tuple3;
import utils.TrajectoryParser;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;

import static utils.TSPGreedy.encodeShapes;
import static utils.TrajPutUtil.getPutWithIndex;
import static utils.TrajPutUtil.getSpatialIndex;
import static client.Constants.DEFAULT_CF;
import static client.Constants.META_TABLE_ADAPTIVE_PARTITION;
import static client.Constants.META_TABLE_LETI_BOUNDARY_XMAX;
import static client.Constants.META_TABLE_LETI_BOUNDARY_XMIN;
import static client.Constants.META_TABLE_LETI_BOUNDARY_YMAX;
import static client.Constants.META_TABLE_LETI_BOUNDARY_YMIN;
import static client.Constants.META_TABLE_MAIN_TABLE_MOVE_BITS;
import static client.Constants.META_TABLE_ORDER_DEFINITION_PATH;

public class LetiLoader extends Loader {
    private static final String LETI_UNION_MASK_SUFFIX = ":leti:union_mask";
    private static final String LETI_SHAPES_SUFFIX = ":leti:shapes";

    LSFCReader.EffectiveNodeIndex effectiveNodeIndex;
    int mainTableMoveBits;

    /**
     * 构造函数：初始化 LETI 索引加载器
     *
     * @param config 表配置
     * @param tableName 表名
     * @param sourcePath 源数据路径
     * @param resultPath 结果输出路径
     * @throws IOException 文件读取异常
     */
    public LetiLoader(TableConfig config, String tableName, String sourcePath, String resultPath) throws IOException {
        super(config, tableName, sourcePath, resultPath);
        // 设置 LETI 索引的固定配置
        config.setIsXZ(0);
        config.setOrderEncodingType(1);
        config.setTspEncoding(1);
//        config.setAdaptivePartition(1);
        String orderingPath = LetiOrderResolver.resolveClasspathResource(config.getOrderDefinitionPath());
        config.setOrderDefinitionPath(orderingPath);
        LSFCReader.EffectiveNodeIndex mapper = LSFCReader.loadEffectiveOnlyFromClasspath(orderingPath);
        effectiveNodeIndex = mapper;
        mainTableMoveBits = resolveMainTableMoveBits(mapper.metadata);
        config.setMainTableMoveBits(mainTableMoveBits);
        applyLetiMeta(config, orderingPath, mapper);
    }

    /**
     * 构造函数：使用配置中的表名
     * 自动设置 LETI 索引的固定配置：
     * - isXZPlus = 0（不使用 XZ 编码）
     * - orderEncodingType = 1（使用 RL 编码）
     * - tspEncoding = 1（使用 TSP 编码）
     *
     * @param config 表配置
     * @param sourcePath 源数据路径
     * @param resultPath 结果输出路径
     * @throws IOException 文件读取异常
     */
    public LetiLoader(TableConfig config, String sourcePath, String resultPath) throws IOException {
        super(config, sourcePath, resultPath);

        // 设置 LETI 索引的固定配置
        config.setIsXZ(0);
        config.setOrderEncodingType(1);
        config.setTspEncoding(1);
//        config.setAdaptivePartition(1);

        String orderingPath = LetiOrderResolver.resolveClasspathResource(config.getOrderDefinitionPath());
        config.setOrderDefinitionPath(orderingPath);
        LSFCReader.EffectiveNodeIndex letiOrderData = LSFCReader.loadEffectiveOnlyFromClasspath(orderingPath);
        effectiveNodeIndex = letiOrderData;
        mainTableMoveBits = resolveMainTableMoveBits(letiOrderData.metadata);
        config.setMainTableMoveBits(mainTableMoveBits);
        applyLetiMeta(config, orderingPath, letiOrderData);
    }

    private static void applyLetiMeta(TableConfig config,
                                      String orderingPath,
                                      LSFCReader.EffectiveNodeIndex mapper) {
        LetiOrderResolver.LetiOrderDescriptor descriptor = LetiOrderResolver.resolveDescriptor(orderingPath);
        config.setOrderDefinitionPath(descriptor.canonicalPath);
        config.setLetiOrderName(descriptor.orderName);
        config.setLetiOrderDataset(descriptor.dataset);
        config.setLetiOrderDistribution(descriptor.distribution);

        if (mapper.metadata != null) {
            config.setLetiOrderVersion(mapper.metadata.version);
            config.setLetiOrderCount(mapper.metadata.orderCount);
            config.setLetiActiveCells(mapper.metadata.activeCells);
            config.setLetiTotalCells(mapper.metadata.totalCells);
            config.setLetiMaxLevel(mapper.metadata.maxLevel);
            config.setLetiGlobalAlpha(mapper.metadata.globalAlpha);
            config.setLetiGlobalBeta(mapper.metadata.globalBeta);
            if (mapper.metadata.boundary != null) {
                config.setLetiOrderBoundary(new Envelope(
                        mapper.metadata.boundary.getXMin(),
                        mapper.metadata.boundary.getYMin(),
                        mapper.metadata.boundary.getXMax(),
                        mapper.metadata.boundary.getYMax()
                ));
            }
        }
    }

    /**
     * 获取轨迹索引和父 quad shape
     * 一次解析 WKT 字符串，同时计算：
     * - 基础空间索引（level, quadCode, shapeCode）
     * - 父 quad 中的 shape（shapeCodeInParent）
     *
     * @param traj 轨迹字符串（格式：oid-tid-wkt）
     * @return TrajIndexResult 包含所有索引信息
     */
    private static TrajIndexResult getTrajIndexWithParentShape(String traj,
                                                               TableConfig config,
                                                               LSFCReader.EffectiveNodeIndex effectiveNodeIndex) {

        TrajectoryParser.ParsedTrajectory pt = TrajectoryParser.parse(traj);

        // 计算基础空间索引
        Tuple3<Object, Object, Object> idx = getSpatialIndex(pt.esriGeo, config);
        int level = (int) idx._1();
        long quadCode = (long) idx._2();
        long shapeCode = (long) idx._3();

        // 获取父 quad 信息
        LSFCReader.EffectiveNodeEntry effectiveNode = effectiveNodeIndex.resolveNearestEffectiveParent(quadCode);
        if (effectiveNode == null || effectiveNode.parentQuad == null) {
            throw new RuntimeException("Parent quad not found for quadCode: " + quadCode + ", trajectory: " + traj);
        }
        LSFCReader.ParentQuad parentQuad = effectiveNode.parentQuad;

        // 计算父 quad 中的 shape
        long shapeCodeInParent = calculateShapeInParentEE(parentQuad, pt.esriGeo, config);

        return new TrajIndexResult(level, quadCode, effectiveNode.elementCode, effectiveNode.order,
                shapeCode, shapeCodeInParent, parentQuad, pt.esriGeo);
    }

    private static ShapeEncodingInfo buildShapeEncodingInfo(ShapeSetAgg agg, int tspEncoding) {
        List<Long> shapeList = new ArrayList<>(agg.shapes);
        List<Integer> shapeOrders = encodeShapes(shapeList, tspEncoding);
        Map<Long, Integer> shapeOrderMap = new HashMap<>();
        long[] orderedShapeCodes = new long[shapeOrders.size()];
        int[] orderByOriginalIndex = new int[shapeOrders.size()];
        Arrays.fill(orderByOriginalIndex, -1);

        for (int shapeOrder = 0; shapeOrder < shapeOrders.size(); shapeOrder++) {
            int originalIndex = shapeOrders.get(shapeOrder);
            if (originalIndex < 0 || originalIndex >= shapeList.size()) {
                throw new IllegalStateException("Invalid shape order index: " + originalIndex);
            }
            orderByOriginalIndex[originalIndex] = shapeOrder;
        }

        for (int i = 0; i < shapeList.size(); i++) {
            int shapeOrder = orderByOriginalIndex[i];
            if (shapeOrder < 0) {
                throw new IllegalStateException("Shape index missing from encoded order: index=" + i);
            }
            long shapeCode = shapeList.get(i);
            shapeOrderMap.put(shapeCode, shapeOrder);
            orderedShapeCodes[shapeOrder] = shapeCode;
        }

        return new ShapeEncodingInfo(shapeOrderMap, packOrderedShapeCodes(orderedShapeCodes), agg.unionMask);
    }

    private static byte[] packOrderedShapeCodes(long[] orderedShapeCodes) {
        byte[] packed = new byte[orderedShapeCodes.length * Long.BYTES];
        for (int i = 0; i < orderedShapeCodes.length; i++) {
            ByteArraysWrapper.writeLong(orderedShapeCodes[i], packed, i * Long.BYTES);
        }
        return packed;
    }

    /**
     * 轨迹索引结果类
     * 包含基础索引信息和父 quad shape 信息
     */
    private static class TrajIndexResult implements Serializable {
        final int level;
        final long quadCode;
        final long effectiveQuadCode;
        final int quadOrderInParent;
        final long shapeCode;
        final long shapeCodeInParent;
        final LSFCReader.ParentQuad parentQuad;
        final MultiPoint geo;

        TrajIndexResult(int level, long quadCode, long effectiveQuadCode, int quadOrderInParent,
                        long shapeCode, long shapeCodeInParent,
                       LSFCReader.ParentQuad parentQuad, MultiPoint geo) {
            this.level = level;
            this.quadCode = quadCode;
            this.effectiveQuadCode = effectiveQuadCode;
            this.quadOrderInParent = quadOrderInParent;
            this.shapeCode = shapeCode;
            this.shapeCodeInParent = shapeCodeInParent;
            this.parentQuad = parentQuad;
            this.geo = geo;
        }
    }

    @Override
    protected void appendIndexMeta(Put put) {
        if (config.getOrderDefinitionPath() != null && !config.getOrderDefinitionPath().isEmpty()) {
            put.addColumn(Bytes.toBytes(DEFAULT_CF),
                    Bytes.toBytes(META_TABLE_ORDER_DEFINITION_PATH),
                    Bytes.toBytes(config.getOrderDefinitionPath()));
        }
        put.addColumn(Bytes.toBytes(DEFAULT_CF),
                Bytes.toBytes(META_TABLE_ADAPTIVE_PARTITION),
                Bytes.toBytes(config.getAdaptivePartition()));
        put.addColumn(Bytes.toBytes(DEFAULT_CF),
                Bytes.toBytes(META_TABLE_MAIN_TABLE_MOVE_BITS),
                Bytes.toBytes(config.getMainTableMoveBits()));
        if (config.getLetiOrderBoundary() != null) {
            put.addColumn(Bytes.toBytes(DEFAULT_CF), Bytes.toBytes(META_TABLE_LETI_BOUNDARY_XMIN), Bytes.toBytes(config.getLetiOrderBoundary().getXMin()));
            put.addColumn(Bytes.toBytes(DEFAULT_CF), Bytes.toBytes(META_TABLE_LETI_BOUNDARY_XMAX), Bytes.toBytes(config.getLetiOrderBoundary().getXMax()));
            put.addColumn(Bytes.toBytes(DEFAULT_CF), Bytes.toBytes(META_TABLE_LETI_BOUNDARY_YMIN), Bytes.toBytes(config.getLetiOrderBoundary().getYMin()));
            put.addColumn(Bytes.toBytes(DEFAULT_CF), Bytes.toBytes(META_TABLE_LETI_BOUNDARY_YMAX), Bytes.toBytes(config.getLetiOrderBoundary().getYMax()));
        }
    }

    private static int resolveMainTableMoveBits(LSFCReader.IndexMeta metadata) {
        if (metadata != null && metadata.maxShapes > 0) {
            return Math.max(1, 64 - Long.numberOfLeadingZeros(metadata.maxShapes));
        }
        return 1;
    }

    /**
     * 计算轨迹在父 quad 的扩展单元格（EE）中的签名
     * 计算逻辑：
     * 1. parentQuad 的边界定义基础单元格（cell）
     * 2. EE 边界 = cell 向右上扩展（全局 alpha * cellWidth, 全局 beta * cellHeight）
     * 3. 在 EE 内划分网格：
     *    - 全局划分：使用全局 alpha 和 beta
     *    - 自适应划分：使用 parentQuad.alpha 和 parentQuad.beta
     * 4. 返回轨迹覆盖的网格单元位掩码
     *
     * @param parentQuad 父 quad 信息（包含基础单元格边界和自适应参数）
     * @param geo 轨迹几何对象（已解析）
     * @return 签名位掩码，每一位表示一个网格单元是否被轨迹覆盖
     */
    private static long calculateShapeInParentEE(LSFCReader.ParentQuad parentQuad, MultiPoint geo, TableConfig config) {

        double cellXmin = parentQuad.getXmin();
        double cellYmin = parentQuad.getYmin();
        double cellWidth = parentQuad.getXmax() - cellXmin;
        double cellHeight = parentQuad.getYmax() - cellYmin;

        double eeXmax = cellXmin + config.getAlpha() * cellWidth;
        double eeYmax = cellYmin + config.getBeta() * cellHeight;

        int partitionAlpha, partitionBeta;
        if (config.isAdaptivePartition()) {
            partitionAlpha = parentQuad.getAlpha();
            partitionBeta = parentQuad.getBeta();
        } else {
            partitionAlpha = config.getAlpha();
            partitionBeta = config.getBeta();
        }

        double gridCellW = (eeXmax - cellXmin) / partitionAlpha;
        double gridCellH = (eeYmax - cellYmin) / partitionBeta;

        long signature = 0L;
        for (int i = 0; i < partitionAlpha; i++) {
            for (int j = 0; j < partitionBeta; j++) {
                double minX = cellXmin + gridCellW * i;
                double minY = cellYmin + gridCellH * j;
                Envelope env = new Envelope(minX, minY, minX + gridCellW, minY + gridCellH);
                if (OperatorIntersects.local().execute(env, geo, SpatialReference.create(4326), null)) {
                    signature |= (1L << (i * partitionBeta + j));
                }
            }
        }

        return signature;
    }

    /**
     * 校验轨迹的 MBR 是否完全包含在父 quad 的扩展单元格（EE）中（使用已解析的几何对象）
     *
     * @param geo 轨迹几何对象（已解析）
     * @param parentQuad 父 quad 信息
     * @param level 四叉树层级
     * @param quadCode 四叉树编码
     * @param shapeCode 形状编码
     * @throws AssertionError 如果轨迹 MBR 不在 EE 内
     */
    private static void assertTrajMBRInsideParentEE(MultiPoint geo,
                                                    LSFCReader.ParentQuad parentQuad,
                                                    TableConfig config,
                                                    int level, long quadCode, long shapeCode) {
        if (parentQuad == null) {
            throw new RuntimeException("ParentEE is null for quadCode: " + quadCode);
        }

        Envelope2D mbr = new Envelope2D();
        geo.queryEnvelope2D(mbr);

        double cellXmin = parentQuad.getXmin();
        double cellYmin = parentQuad.getYmin();
        double cellWidth = parentQuad.getXmax() - cellXmin;
        double cellHeight = parentQuad.getYmax() - cellYmin;

        double eeXmax = cellXmin + config.getAlpha() * cellWidth;
        double eeYmax = cellYmin + config.getBeta() * cellHeight;

        if (!(mbr.xmin >= cellXmin &&
                mbr.ymin >= cellYmin &&
                mbr.xmax <= eeXmax &&
                mbr.ymax <= eeYmax)) {

            throw new AssertionError(
                    "\n[ASSERT FAILED] Trajectory MBR not inside parentEE\n" +
                            "MBR      = (" + mbr.xmin + ", " + mbr.ymin +
                            ", " + mbr.xmax + ", " + mbr.ymax + ")\n" +
                            "parentEE = (" + cellXmin + ", " + cellYmin +
                            ", " + eeXmax + ", " + eeYmax + ")\n" +
                            "parentQuad.level = " + parentQuad.getLevel() + "\n" +
                            "level  = " + level + "\n" +
                            "quadCode = " + quadCode + "\n" +
                            "shapeCode= " + shapeCode
            );
        }
    }

    @Override
    public void deleteSecondaryTable() {
        super.deleteSecondaryTable();
        try (Jedis jedis = new Jedis(config.getRedisHost(), 6379, 60000)) {
            Pipeline pipelined = jedis.pipelined();
            pipelined.del(tableName + LETI_UNION_MASK_SUFFIX);
            pipelined.del(tableName + LETI_SHAPES_SUFFIX);
            pipelined.sync();
            pipelined.close();
        }
    }

    @Override
    void storePrimaryTable() throws IOException {
        SparkConf conf = new SparkConf()
                .setMaster("local[*]")
                .setAppName(Loader.class.getSimpleName() + tableName)
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        conf.registerKryoClasses(new Class[]{Put.class, Tuple2.class});

        createTrajTable();
        deleteSecondaryTable();

        long currentTime = System.currentTimeMillis();

        try (JavaSparkContext context = new JavaSparkContext(conf)) {
            JavaRDD<String> rawTrajRDD = context.textFile(sourcePath);
            long trajectoryCount = rawTrajRDD.count();

            int rowKeyMoveBits = mainTableMoveBits;
            final TableConfig taskConfig = config;
            final String taskTableName = tableName;
            final Broadcast<LSFCReader.EffectiveNodeIndex> effectiveNodeIndexBc = context.broadcast(effectiveNodeIndex);

            JavaPairRDD<Long, RawTrajOrderInfo> qOrderToTrajInfoRDD = rawTrajRDD.mapToPair(rawTraj -> {
                LSFCReader.EffectiveNodeIndex localEffectiveNodeIndex = effectiveNodeIndexBc.value();
                TrajIndexResult indexResult = getTrajIndexWithParentShape(rawTraj, taskConfig, localEffectiveNodeIndex);

                long quadOrderInParent = indexResult.quadOrderInParent;
                LSFCReader.ParentQuad parentQuad = indexResult.parentQuad;

                assert parentQuad != null;
                assert parentQuad.elementCode == indexResult.effectiveQuadCode;

//                assertTrajMBRInsideParentEE(indexResult.geo, parentQuad, taskConfig,
//                        indexResult.level, quadCode, indexResult.shapeCode);

                return new Tuple2<>(quadOrderInParent, new RawTrajOrderInfo(indexResult.shapeCodeInParent, rawTraj));
            }).persist(StorageLevel.DISK_ONLY());

            JavaPairRDD<Long, Long> qOrderToShapeRDD = qOrderToTrajInfoRDD.mapToPair(entry ->
                    new Tuple2<>(entry._1(), entry._2().shapeCodeInParent));

            JavaPairRDD<Long, ShapeSetAgg> aggregatedShapesByOrderRDD =
                    qOrderToShapeRDD.aggregateByKey(new ShapeSetAgg(), ShapeSetAgg::add, ShapeSetAgg::merge)
                            .persist(StorageLevel.DISK_ONLY());

            JavaPairRDD<Long, ShapeEncodingInfo> shapeEncodingInfoByQOrderRDD =
                    aggregatedShapesByOrderRDD.mapValues(agg -> buildShapeEncodingInfo(agg, config.getTspEncoding()))
                            .persist(StorageLevel.DISK_ONLY());

            Map<Long, ShapeEncodingInfo> shapeEncodingInfoMap = shapeEncodingInfoByQOrderRDD.collectAsMap();
            final Broadcast<Map<Long, ShapeEncodingInfo>> shapeEncodingInfoBc = context.broadcast(shapeEncodingInfoMap);

            JavaRDD<Tuple3<Put, Map<String, Tuple2<Long, byte[]>>, List<KeyValue>>> indexedRDD =
                    qOrderToTrajInfoRDD
                            .map(tuple -> {
                                long quadOrder = tuple._1();
                                RawTrajOrderInfo rawInfo = tuple._2();
                                ShapeEncodingInfo shapeEncodingInfo = shapeEncodingInfoBc.value().get(quadOrder);
                                if (shapeEncodingInfo == null) {
                                    throw new IllegalStateException("Shape encoding info missing for qOrder=" + quadOrder);
                                }
                                Map<Long, Integer> shapeOrderMap = shapeEncodingInfo.shapeOrderMap;

                                Integer shapeOrder = shapeOrderMap.get(rawInfo.shapeCodeInParent);
                                if (shapeOrder == null) {
                                    throw new IllegalStateException("Shape order missing for qOrder=" + quadOrder
                                            + ", shapeCode=" + rawInfo.shapeCodeInParent);
                                }

                                long rowKeyIndex = (quadOrder << rowKeyMoveBits) | shapeOrder;
                                Tuple3<Put, Long, List<KeyValue>> putWithIndex = getPutWithIndex(rawInfo.rawTraj,
                                        rowKeyIndex, taskConfig);

                                Put put = putWithIndex._1();
                                List<KeyValue> keyValues = putWithIndex._3();

                                Map<String, Tuple2<Long, byte[]>> secondaryIndexMap = new HashMap<>();
                                secondaryIndexMap.put(taskTableName, new Tuple2<>(quadOrder, Bytes.toBytes(shapeOrder)));

                                return new Tuple3<>(put, secondaryIndexMap, keyValues);
                            })
                            .persist(StorageLevel.DISK_ONLY());
            long mainTableBytes = sumPrimaryTableBytes(indexedRDD);

            try (Jedis jedis = new Jedis(config.getRedisHost(), 6379, 60000)) {
                int size = 0;
                Pipeline pipelined = jedis.pipelined();
                String unionMaskKey = tableName + LETI_UNION_MASK_SUFFIX;
                byte[] shapeCacheKey = Bytes.toBytes(tableName + LETI_SHAPES_SUFFIX);
                for (Map.Entry<Long, ShapeEncodingInfo> entry : shapeEncodingInfoMap.entrySet()) {
                    long quadOrder = entry.getKey();
                    ShapeEncodingInfo encodingInfo = entry.getValue();
                    pipelined.hset(unionMaskKey, Long.toString(quadOrder), Long.toString(encodingInfo.unionMask));
                    pipelined.hset(shapeCacheKey, Bytes.toBytes(quadOrder), encodingInfo.orderedShapeCodes);
                    if (++size % 2000 == 0) {
                        pipelined.sync();
                    }
                }
                pipelined.sync();
                pipelined.close();
            }

            storePrimaryTableWithHadoopDataset(indexedRDD);
            /*
                        int size = 0;
                        Jedis jedis = new Jedis(config.getRedisHost(), 6379, 60000);
                        Pipeline pipelined = jedis.pipelined();
                        while (v.hasNext()) {
                            Tuple2<Long, Tuple2<Integer, byte[]>> entry = v.next();

                            long originIndex = entry._1;

                            Tuple2<Integer, byte[]> indexPayload = entry._2;
                            int count = indexPayload._1;
                            byte[] shapeOrder = indexPayload._2;

                            byte[] indexedSize = new byte[16];
                            ByteArraysWrapper.writeInt(count, indexedSize, 0);
                            System.arraycopy(shapeOrder, 0, indexedSize, 4, 4);
                            ByteArraysWrapper.writeLong(originIndex, indexedSize, 8);

                            pipelined.zadd(Bytes.toBytes(tableName), originIndex, indexedSize);
                            if (++size % 2000 == 0) {
                                pipelined.sync();
                                System.out.printf("[Loader.storePrimaryTable] Redis 存储进度: %6d 项%n", size);
                            }
                        }
                        pipelined.sync();
                        pipelined.sync();
                        pipelined.close();
                        jedis.close();
                    });
            */
            currentTime = System.currentTimeMillis() - currentTime;
            System.out.println("[Loader.storePrimaryTable] 主表存储完成，耗时: " + currentTime + " ms");
            storeSecondaryTable(indexedRDD);
            StoreSummary summary = new StoreSummary();
            summary.setTrajectoryCount(trajectoryCount);
            summary.setNodeCount(effectiveNodeIndex.metadata != null && effectiveNodeIndex.metadata.activeCells > 0
                    ? effectiveNodeIndex.metadata.activeCells
                    : effectiveNodeIndex.entries.size());
            summary.setShapeCount(shapeEncodingInfoMap.values().stream()
                    .mapToLong(v -> v.orderedShapeCodes.length / Long.BYTES)
                    .sum());
            summary.setIndexingTimeMs(currentTime);
            summary.setMainTableBytes(mainTableBytes);
            summary.setIndexTableBytes(shapeEncodingInfoMap.values().stream()
                    .mapToLong(v -> v.orderedShapeCodes.length + (Long.BYTES * 2L))
                    .sum());
            summary.setExtraIndexInfoBytes(summary.getIndexTableBytes());
            printAndPersistStoreSummary(summary);
            indexedRDD.unpersist(false);
            qOrderToTrajInfoRDD.unpersist(false);
            shapeEncodingInfoBc.destroy();
            shapeEncodingInfoByQOrderRDD.unpersist(false);
            aggregatedShapesByOrderRDD.unpersist(false);
            effectiveNodeIndexBc.destroy();
            System.out.println("[Loader.storePrimaryTable] 二级索引表存储完成");
        }
    }

    static class RawTrajOrderInfo implements Serializable {
        final long shapeCodeInParent;
        final String rawTraj;

        RawTrajOrderInfo(long shapeCodeInParent, String rawTraj) {
            this.shapeCodeInParent = shapeCodeInParent;
            this.rawTraj = rawTraj;
        }
    }

    /**
     * 轨迹形状聚合器：用于按 quadOrder 聚合轨迹和形状
     */
    static class ShapeSetAgg implements Serializable {
        Set<Long> shapes = new HashSet<>();
        long unionMask = 0L;

        ShapeSetAgg add(Long shape) {
            shapes.add(shape);
            unionMask |= shape;
            return this;
        }

        ShapeSetAgg merge(ShapeSetAgg other) {
            shapes.addAll(other.shapes);
            unionMask |= other.unionMask;
            return this;
        }
    }

    private static class ShapeEncodingInfo implements Serializable {
        final Map<Long, Integer> shapeOrderMap;
        final byte[] orderedShapeCodes;
        final long unionMask;

        private ShapeEncodingInfo(Map<Long, Integer> shapeOrderMap, byte[] orderedShapeCodes, long unionMask) {
            this.shapeOrderMap = shapeOrderMap;
            this.orderedShapeCodes = orderedShapeCodes;
            this.unionMask = unionMask;
        }
    }
}
