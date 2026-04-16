package utils;

import com.esri.core.geometry.*;
import client.Constants;
import config.TableConfig;
import constans.IndexEnum;
import entity.Trajectory;
import index.CellIndex;
import index.LETILocSIndex;
import index.LMSFCIndex;
import index.LocSIndex;
import index.XZStarIndex;
import index.XZLocSIndex;
import index.XZSFC;
import preprocess.compress.IIntegerCompress;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;
import org.locationtech.jts.io.WKTWriter;
import scala.Tuple2;
import scala.Tuple3;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.stream.Collectors;

/**
 * 轨迹数据 Put 操作工具类
 * <p>
 * 提供将轨迹数据转换为 HBase Put 操作的工具方法，包括：
 * - 轨迹几何数据的编码（经纬度、时间）
 * - RowKey 的构造（索引值 + 轨迹ID，可选分片前缀）
 * - HBase Put 和 KeyValue 的生成
 * - 空间索引的计算（LocSIndex、XZLocSIndex、CellIndex）
 *
 * @author hehuajun
 */
public class TrajPutUtil implements Serializable {
    /**
     * WKT 写入器，用于将几何对象转换为 WKT 字符串
     */
    public static final WKTWriter writer = new WKTWriter(3);
    /**
     * WKT 读取器，用于解析 WKT 字符串
     */
    public static final WKTReader reader = new WKTReader();

    /**
     * 构造 Put 操作（包含索引和 KeyValues）
     * <p>
     * 根据位置编码和形状编码计算索引值，构造 RowKey 并生成 Put 操作。
     * RowKey 格式：[index(8字节)][tid]，可选分片前缀 [shard(1字节)][index(8字节)][tid]
     *
     * @param traj 轨迹字符串（格式：oid-tid-wkt）
     * @param location 位置编码（quadCode）
     * @param order 形状编码（shapeOrder，TSP 编码后的值）
     * @param config 表配置
     * @return Tuple3<Put, Long, List < KeyValue>> Put 操作、索引值、KeyValue 列表
     * @throws ParseException WKT 解析异常
     */
    public static Tuple3<Put, Long, List<KeyValue>> getPutWithIndex(String traj, Long location, Integer order, TableConfig config) throws ParseException {
        String[] t = traj.split("-");
        int moveBits = config.getAlpha() * config.getBeta();
        long index = order.longValue() | (location << moveBits);
        String tid = t[1];
        tid = tid + (int) (Math.random() * 20);

        byte[] rowkey = buildRowkey(index, tid, config);
        return getPutWithIndex(traj, new Tuple2<>(index, rowkey), config);
    }

    /**
     * 构造 Put 操作（核心方法）
     * <p>
     * 将轨迹数据编码并构造 HBase Put 操作，包括：
     * - 几何数据编码（经纬度、时间）
     * - 元数据提取（起止点、起止时间、DP 特征）
     * - Put 和 KeyValue 的生成
     *
     * @param traj 轨迹字符串（格式：oid-tid-wkt）
     * @param indexValue Tuple2<索引值, RowKey字节数组>
     * @param config 表配置
     * @return Tuple3<Put, Long, List < KeyValue>> Put 操作、索引值、KeyValue 列表
     * @throws ParseException WKT 解析异常
     */
    public static Tuple3<Put, Long, List<KeyValue>> getPutWithIndex(String traj, Tuple2<Long, byte[]> indexValue, TableConfig config) throws ParseException {
        String[] t = traj.split("-");
        MultiPoint geo = parseWKTToMultiPoint(t[2]);
        org.locationtech.jts.geom.Geometry geoJTS = reader.read(t[2]);

        int[] xValue = new int[geo.getPointCount()];
        int[] yValue = new int[geo.getPointCount()];
        int[] zValue = new int[geo.getPointCount()];
        encodeGeometry(geo, xValue, yValue, zValue);

        IIntegerCompress integerCompress = IIntegerCompress.getIntegerCompress(config.getCompressType().name());
        byte[] xEncoding = integerCompress.encoding(xValue);
        byte[] yEncoding = integerCompress.encoding(yValue);
        byte[] zEncoding = integerCompress.encoding(zValue);

        byte[] rowkey = indexValue._2;
        Trajectory trajectory = new Trajectory(t[0], t[1], geoJTS);

        Put put = new Put(rowkey);
        List<KeyValue> keyValueList = new ArrayList<>();

        addColumnsToPut(put, t[0], t[1], geo, geoJTS, trajectory, xEncoding, yEncoding, zEncoding);
        addColumnsToKeyValueList(keyValueList, rowkey, t[0], t[1], geo, geoJTS, trajectory, xEncoding, yEncoding, zEncoding);

        return new Tuple3<>(put, indexValue._1, keyValueList);
    }

    /**
     * 构造 Put 操作（使用已有索引值）
     * <p>
     * 当调用方已持有索引值时，直接构造 RowKey 并生成 Put 操作。
     * RowKey 格式：[index(8字节)][tid]，可选分片前缀
     *
     * @param traj 轨迹字符串（格式：oid-tid-wkt）
     * @param indexValue 索引值
     * @param config 表配置
     * @return Tuple3<Put, Long, List < KeyValue>> Put 操作、索引值、KeyValue 列表
     * @throws ParseException WKT 解析异常
     */
    public static Tuple3<Put, Long, List<KeyValue>> getPutWithIndex(String traj, long indexValue, TableConfig config) throws ParseException {
        String[] t = traj.split("-");
        String tid = t[1] + (int) (Math.random() * 20);
        byte[] rowkey = buildRowkey(indexValue, tid, config);
        return getPutWithIndex(traj, new Tuple2<>(indexValue, rowkey), config);
    }

    /**
     * 直接计算索引并构造 Put 操作（不包含 KeyValue 列表）
     * <p>
     * 根据表配置的主索引类型计算索引值，构造 RowKey 并生成 Put 操作。
     * 适用于不需要 KeyValue 列表的场景（如不使用 Bulk Load）。
     *
     * @param traj 轨迹字符串（格式：oid-tid-wkt）
     * @param config 表配置
     * @return Tuple2<Put, Long> Put 操作和索引值
     * @throws ParseException WKT 解析异常
     */
    public static Tuple2<Put, Long> getPut(String traj, TableConfig config) throws ParseException {
        String[] t = traj.split("-");
        MultiPoint geo = parseWKTToMultiPoint(t[2]);
        org.locationtech.jts.geom.Geometry geoJTS = reader.read(t[2]);

        int[] xValue = new int[geo.getPointCount()];
        int[] yValue = new int[geo.getPointCount()];
        int[] zValue = new int[geo.getPointCount()];
        encodeGeometry(geo, xValue, yValue, zValue);

        IIntegerCompress integerCompress = IIntegerCompress.getIntegerCompress(config.getCompressType().name());
        byte[] xEncoding = integerCompress.encoding(xValue);
        byte[] yEncoding = integerCompress.encoding(yValue);
        byte[] zEncoding = integerCompress.encoding(zValue);

        Tuple2<Long, byte[]> indexValue = getIndex(t[0], t[1], geo, geoJTS, config);
        byte[] index = indexValue._2;

        Put put = new Put(index);
        put.addColumn(Bytes.toBytes(Constants.DEFAULT_CF), Bytes.toBytes(Constants.O_ID), Bytes.toBytes(Long.parseLong(t[0])));
        put.addColumn(Bytes.toBytes(Constants.DEFAULT_CF), Bytes.toBytes(Constants.T_ID), Bytes.toBytes(t[1]));
        put.addColumn(Bytes.toBytes(Constants.DEFAULT_CF), Bytes.toBytes(Constants.GEOM_X), xEncoding);
        put.addColumn(Bytes.toBytes(Constants.DEFAULT_CF), Bytes.toBytes(Constants.GEOM_Y), yEncoding);
        put.addColumn(Bytes.toBytes(Constants.DEFAULT_CF), Bytes.toBytes(Constants.GEOM_Z), zEncoding);
        Trajectory trajectory = new Trajectory(t[0], t[1], geoJTS);
        put.addColumn(Bytes.toBytes(Constants.DEFAULT_CF), Bytes.toBytes(Constants.START_POINT), Bytes.toBytes(writer.write(geoJTS.getGeometryN(0))));
        put.addColumn(Bytes.toBytes(Constants.DEFAULT_CF), Bytes.toBytes(Constants.END_POINT), Bytes.toBytes(writer.write(geoJTS.getGeometryN(geoJTS.getNumPoints() - 1))));
        put.addColumn(Bytes.toBytes(Constants.DEFAULT_CF), Bytes.toBytes(Constants.START_TIME), Bytes.toBytes((long) geo.getPoint(0).getZ()));
        put.addColumn(Bytes.toBytes(Constants.DEFAULT_CF), Bytes.toBytes(Constants.END_TIME), Bytes.toBytes((long) geo.getPoint(geo.getPointCount() - 1).getZ()));
        put.addColumn(Bytes.toBytes(Constants.DEFAULT_CF), Bytes.toBytes(Constants.PIVOT_POINT), Bytes.toBytes(trajectory.getDPFeature().getIndexes().stream().map(Object::toString).collect(Collectors.joining(","))));
        put.addColumn(Bytes.toBytes(Constants.DEFAULT_CF), Bytes.toBytes(Constants.PIVOT_MBR), Bytes.toBytes(trajectory.getDPFeature().getMBRs().toText()));
        return new Tuple2<>(put, indexValue._1);
    }

    /**
     * 根据主索引类型计算索引值并构造 RowKey
     *
     * @param oid    对象ID
     * @param tid    轨迹ID
     * @param geo    轨迹几何对象
     * @param config 表配置
     * @return Tuple2<索引值, RowKey字节数组>
     */
    private static Tuple2<Long, byte[]> getIndex(String oid, String tid, MultiPoint geo,
                                                 org.locationtech.jts.geom.Geometry geoJTS, TableConfig config) {
        byte[] bytes = new byte[8 + tid.length()];
        long indexValue = 0;
        if (Objects.requireNonNull(config.getPrimary()) == IndexEnum.INDEX_TYPE.SPATIAL) {
            switch (config.getSpatialIndexKind()) {
                case XZ_STAR:
                    indexValue = getXZStarSpatialIndexValue(geoJTS, config);
                    break;
                case XZPlus:
                    indexValue = getXZSpatialIndexValue(geo, config);
                    break;
                case LETI:
                case TShape:
                default:
                    indexValue = getSpatialIndexValue(geo, config);
                    break;
            }
            ByteArraysWrapper.writeLong(indexValue, bytes, 0);
            System.arraycopy(Bytes.toBytes(tid), 0, bytes, 8, tid.length());
        }
        Short shards = config.getShards();
        Random random = new Random();
        if (null != shards) {
            byte[] finalBytes = new byte[bytes.length + 1];
            finalBytes[0] = (byte) (random.nextInt(shards));
            System.arraycopy(bytes, 0, finalBytes, 1, bytes.length);
            return new Tuple2<>(indexValue, finalBytes);
        }
        return new Tuple2<>(indexValue, bytes);
    }

    /**
     * 计算轨迹的空间索引
     * <p>
     * 根据配置选择使用 LocSIndex、XZLocSIndex 或 LETILocSIndex 计算空间索引。
     * 索引类型选择规则：
     * - 如果启用 RL Encoding，使用 LETILocSIndex
     * - 如果使用 XZ 编码，使用 XZLocSIndex
     * - 否则使用 LocSIndex
     *
     * @param traj 轨迹字符串（格式：oid-tid-wkt）
     * @param config 表配置
     * @return Tuple3<level, quadCode, shapeCode> 四叉树层级、位置编码、形状编码
     */
    public static Tuple3<Object, Object, Object> getSpatialIndex(String traj, TableConfig config) {
        String[] t = traj.split("-");
        MultiPoint geo = parseWKTToMultiPoint(t[2]);

        return getSpatialIndex(geo, config);
    }

    /**
     * 计算轨迹的空间索引（传入地理空间对象）
     * <p>
     * 根据配置选择使用 LocSIndex、XZLocSIndex 或 LETILocSIndex 计算空间索引。
     * 索引类型选择规则：
     * - 如果启用 RL Encoding，使用 LETILocSIndex
     * - 如果使用 XZ 编码，使用 XZLocSIndex
     * - 否则使用 LocSIndex
     *
     * @param geo 已解析的轨迹几何对象
     * @param config 表配置
     * @return Tuple3<level, quadCode, shapeCode> 四叉树层级、位置编码、形状编码
     */
    public static Tuple3<Object, Object, Object> getSpatialIndex(MultiPoint geo, TableConfig config) {
        XZSFC xzsfc;
        switch (config.getSpatialIndexKind()) {
            case LETI:
                assert !config.isXZPlus();
                xzsfc = createLETILocSIndex(config);
                break;
            case XZPlus:
                xzsfc = createXZLocSIndex(config);
                break;
            case TShape:
                xzsfc = createLocSIndex(config);
                break;
            case LMSFC:
                xzsfc = createLMSFCLocSIndex(config);
                break;
            case XZ_STAR:
            default:
                // XZ_STAR 的主键/候选区间由 XZStarIndex/XZStarSFC 直接生成；
                // 这里返回 Tuple3<level,quadCode,shapeCode> 的接口不适用。
                throw new UnsupportedOperationException("XZ_STAR does not support getSpatialIndex Tuple3 path");
        }
        return xzsfc.index(geo, false);
    }

    /**
     * 计算轨迹的空间索引（指定索引类型）
     *
     * @param traj 轨迹字符串（格式：oid-tid-wkt）
     * @param config 表配置
     * @param type 索引类型：0=LocSIndex, 1=XZLocSIndex, 2=CellIndex
     * @return Tuple3<level, quadCode, shapeCode> 四叉树层级、位置编码、形状编码
     */
    public static Tuple3<Object, Object, Object> getSpatialIndex(String traj, TableConfig config, int type) {
        String[] t = traj.split("-");
        OperatorImportFromWkt importerWKT = (OperatorImportFromWkt) OperatorFactoryLocal.getInstance().getOperator(Operator.Type.ImportFromWkt);
        MultiPoint geo = (MultiPoint) importerWKT.execute(0, Geometry.Type.MultiPoint, t[2], null);
        XZSFC xzsfc = null;
        switch (type) {
            case 0:
                // LocS Index 模式
                xzsfc = createLocSIndex(config);
                break;
            case 1:
                // XZ Index 模式
                xzsfc = createXZLocSIndex(config);
                break;
            case 2:
                // Cell Index 模式
                xzsfc = createCellIndex(config);
                break;
            case 3:
                // LETI Index 模式
                xzsfc = createLETILocSIndex(config);
                break;
            case 4:
                // LMSFC Index 模式
                xzsfc = createLMSFCLocSIndex(config);
                break;
            case 5:
                xzsfc = createBMTreeIndex(config);
                break;
        }
        assert xzsfc != null;
        return xzsfc.index(geo, false);
    }

    /**
     * 计算空间索引值（使用 LocSIndex 或 LETILocSIndex）
     *
     * @param geo 轨迹几何对象
     * @param config 表配置
     * @return 索引值 = shape | (location << moveBits)
     */
    private static long getSpatialIndexValue(MultiPoint geo, TableConfig config) {
        XZSFC sfc;
        if (config.getSpatialIndexKind() == TableConfig.SpatialIndexKind.LETI) {
            sfc = createLETILocSIndex(config);
        } else {
            sfc = createLocSIndex(config);
        }
        Tuple3<Object, Object, Object> sIndex = sfc.index(geo, false);
        long location = (long) sIndex._2();
        long shape = (long) sIndex._3();
        int moveBits = config.getSpatialIndexKind() == TableConfig.SpatialIndexKind.LETI && config.isAdaptivePartition()
                ? config.getMaxShapeBits()
                : config.getAlpha() * config.getBeta();
        return shape | (location << moveBits);
    }

    /**
     * 计算空间索引值（使用 XZLocSIndex）
     *
     * @param geo 轨迹几何对象
     * @param config 表配置
     * @return 索引值 = shape | (location << moveBits)
     */
    private static long getXZSpatialIndexValue(MultiPoint geo, TableConfig config) {
        XZLocSIndex locSIndex = createXZLocSIndex(config);
        Tuple3<Object, Object, Object> sIndex = locSIndex.index(geo, false);
        long location = (long) sIndex._2();
        long shape = (long) sIndex._3();
        int moveBits = config.getAlpha() * config.getBeta();
        return shape | (location << moveBits);
    }

    private static long getXZStarSpatialIndexValue(org.locationtech.jts.geom.Geometry geoJTS, TableConfig config) {
        XZStarIndex xzStarIndex;
        if (config.getEnvelope() != null) {
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
        return xzStarIndex.index(geoJTS, true);
    }


    /**
     * 创建 LocSIndex 实例
     */
    private static LocSIndex createLocSIndex(TableConfig config) {
        if (config.getEnvelope() != null) {
            return LocSIndex.apply(
                    (short) config.getResolution(),
                    new Tuple2<>(config.getEnvelope().getXMin(), config.getEnvelope().getXMax()),
                    new Tuple2<>(config.getEnvelope().getYMin(), config.getEnvelope().getYMax()),
                    config.getAlpha(), config.getBeta());
        } else {
            return LocSIndex.apply((short) config.getResolution(), config.getAlpha(), config.getBeta());
        }
    }

    /**
     * 创建 XZLocSIndex 实例
     */
    private static XZLocSIndex createXZLocSIndex(TableConfig config) {
        if (config.getEnvelope() != null) {
            return XZLocSIndex.apply(
                    (short) config.getResolution(),
                    new Tuple2<>(config.getEnvelope().getXMin(), config.getEnvelope().getXMax()),
                    new Tuple2<>(config.getEnvelope().getYMin(), config.getEnvelope().getYMax()),
                    config.getAlpha(), config.getBeta());
        } else {
            return XZLocSIndex.apply((short) config.getResolution(), config.getAlpha(), config.getBeta());
        }
    }

    /**
     * 创建 LETILocSIndex 实例
     */
    private static LETILocSIndex createLETILocSIndex(TableConfig config) {
        boolean adaptivePartition = config.isAdaptivePartition();
        String orderDefinitionPath = config.getOrderDefinitionPath();
        if (config.getEnvelope() != null) {
            return LETILocSIndex.apply(
                    (short) config.getResolution(),
                    new Tuple2<>(config.getEnvelope().getXMin(), config.getEnvelope().getXMax()),
                    new Tuple2<>(config.getEnvelope().getYMin(), config.getEnvelope().getYMax()),
                    config.getAlpha(),
                    config.getBeta(),
                    adaptivePartition,
                    orderDefinitionPath);
        } else {
            return LETILocSIndex.apply(
                    (short) config.getResolution(),
                    config.getAlpha(),
                    config.getBeta(),
                    adaptivePartition,
                    orderDefinitionPath);
        }
    }

    /**
     * 创建 CellIndex 实例
     */
    private static CellIndex createCellIndex(TableConfig config) {
        if (config.getEnvelope() != null) {
            return CellIndex.apply(
                    (short) config.getResolution(),
                    new Tuple2<>(config.getEnvelope().getXMin(), config.getEnvelope().getXMax()),
                    new Tuple2<>(config.getEnvelope().getYMin(), config.getEnvelope().getYMax()),
                    config.getAlpha(), config.getBeta());
        } else {
            return CellIndex.apply((short) config.getResolution(), config.getAlpha(), config.getBeta());
        }
    }

    /**
     * 创建 LMSFCIndex 实例
     */
    private static LMSFCIndex createLMSFCLocSIndex(TableConfig config) {
        if (config.getEnvelope() != null) {
            return LMSFCIndex.apply(
                    (short) config.getResolution(),
                    new Tuple2<>(config.getEnvelope().getXMin(), config.getEnvelope().getXMax()),
                    new Tuple2<>(config.getEnvelope().getYMin(), config.getEnvelope().getYMax()),
                    config.getThetaConfig());
        } else {
            return LMSFCIndex.apply(
                    (short) config.getResolution(),
                    new Tuple2<>(-180.0, 180.0),
                    new Tuple2<>(-90.0, 90.0),
                    config.getThetaConfig());
        }
    }

    /**
     * 创建 BMTree 索引
     */
    private static XZSFC createBMTreeIndex(TableConfig config) {
        if (config.getEnvelope() != null) {
            return index.BMTreeIndex.apply(
                    (short) config.getResolution(),
                    new scala.Tuple2<>(config.getEnvelope().getXMin(), config.getEnvelope().getXMax()),
                    new scala.Tuple2<>(config.getEnvelope().getYMin(), config.getEnvelope().getYMax()),
                    config.getBMTreeConfigPath(),
                    config.getBMTreeBitLength());
        } else {
            return index.BMTreeIndex.apply(
                    (short) config.getResolution(),
                    new scala.Tuple2<>(-180.0, 180.0),
                    new scala.Tuple2<>(-90.0, 90.0),
                    config.getBMTreeConfigPath(),
                    config.getBMTreeBitLength());
        }
    }

    /**
     * 解析 WKT 字符串为 MultiPoint 对象
     */
    private static MultiPoint parseWKTToMultiPoint(String wkt) {
        OperatorImportFromWkt importerWKT = (OperatorImportFromWkt) OperatorFactoryLocal.getInstance()
                .getOperator(Operator.Type.ImportFromWkt);
        return (MultiPoint) importerWKT.execute(0, Geometry.Type.MultiPoint, wkt, null);
    }

    /**
     * 编码几何数据：将经纬度和时间转换为整数数组
     *
     * @param geo    轨迹几何对象
     * @param xValue 输出：经度数组（乘以 10^6）
     * @param yValue 输出：纬度数组（乘以 10^6）
     * @param zValue 输出：时间数组（除以 1000，转换为秒）
     */
    private static void encodeGeometry(MultiPoint geo, int[] xValue, int[] yValue, int[] zValue) {
        for (int i = 0; i < geo.getPointCount(); i++) {
            Point point = geo.getPoint(i);
            xValue[i] = (int) (point.getX() * Math.pow(10, 6));
            yValue[i] = (int) (point.getY() * Math.pow(10, 6));
            zValue[i] = (int) (point.getZ() / 1000);
        }
    }

    /**
     * 构造 RowKey
     * <p>
     * RowKey 格式：[index(8字节)][tid]，可选分片前缀 [shard(1字节)][index(8字节)][tid]
     *
     * @param index  索引值
     * @param tid    轨迹ID
     * @param config 表配置
     * @return RowKey 字节数组
     */
    static byte[] buildRowkey(long index, String tid, TableConfig config) {
        byte[] tidBytes = Bytes.toBytes(tid);
        byte[] rowkeyCore = new byte[8 + tidBytes.length];
        ByteArraysWrapper.writeLong(index, rowkeyCore, 0);
        System.arraycopy(tidBytes, 0, rowkeyCore, 8, tidBytes.length);

        Short shards = config.getShards();
        if (shards != null) {
            byte[] finalRowkey = new byte[rowkeyCore.length + 1];
            finalRowkey[0] = (byte) (new Random().nextInt(shards));
            System.arraycopy(rowkeyCore, 0, finalRowkey, 1, rowkeyCore.length);
            return finalRowkey;
        }
        return rowkeyCore;
    }

    /**
     * 添加列到 Put 对象
     */
    private static void addColumnsToPut(Put put, String oid, String tid, MultiPoint geo,
                                        org.locationtech.jts.geom.Geometry geoJTS, Trajectory trajectory,
                                        byte[] xEncoding, byte[] yEncoding, byte[] zEncoding) {
        put.addColumn(Bytes.toBytes(Constants.DEFAULT_CF), Bytes.toBytes(Constants.O_ID), Bytes.toBytes(Long.parseLong(oid)));
        put.addColumn(Bytes.toBytes(Constants.DEFAULT_CF), Bytes.toBytes(Constants.T_ID), Bytes.toBytes(tid));
        put.addColumn(Bytes.toBytes(Constants.DEFAULT_CF), Bytes.toBytes(Constants.GEOM_X), xEncoding);
        put.addColumn(Bytes.toBytes(Constants.DEFAULT_CF), Bytes.toBytes(Constants.GEOM_Y), yEncoding);
        put.addColumn(Bytes.toBytes(Constants.DEFAULT_CF), Bytes.toBytes(Constants.GEOM_Z), zEncoding);
        put.addColumn(Bytes.toBytes(Constants.DEFAULT_CF), Bytes.toBytes(Constants.START_POINT), Bytes.toBytes(writer.write(geoJTS.getGeometryN(0))));
        put.addColumn(Bytes.toBytes(Constants.DEFAULT_CF), Bytes.toBytes(Constants.END_POINT), Bytes.toBytes(writer.write(geoJTS.getGeometryN(geoJTS.getNumPoints() - 1))));
        put.addColumn(Bytes.toBytes(Constants.DEFAULT_CF), Bytes.toBytes(Constants.START_TIME), Bytes.toBytes((long) geo.getPoint(0).getZ()));
        put.addColumn(Bytes.toBytes(Constants.DEFAULT_CF), Bytes.toBytes(Constants.END_TIME), Bytes.toBytes((long) geo.getPoint(geo.getPointCount() - 1).getZ()));
        put.addColumn(Bytes.toBytes(Constants.DEFAULT_CF), Bytes.toBytes(Constants.PIVOT_POINT), Bytes.toBytes(trajectory.getDPFeature().getIndexes().stream().map(Object::toString).collect(Collectors.joining(","))));
        put.addColumn(Bytes.toBytes(Constants.DEFAULT_CF), Bytes.toBytes(Constants.PIVOT_MBR), Bytes.toBytes(trajectory.getDPFeature().getMBRs().toText()));
    }

    /**
     * 添加列到 KeyValue 列表
     */
    private static void addColumnsToKeyValueList(List<KeyValue> keyValueList, byte[] rowkey, String oid, String tid,
                                                 MultiPoint geo, org.locationtech.jts.geom.Geometry geoJTS,
                                                 Trajectory trajectory, byte[] xEncoding, byte[] yEncoding, byte[] zEncoding) {
        keyValueList.add(new KeyValue(rowkey, Bytes.toBytes(Constants.DEFAULT_CF), Bytes.toBytes(Constants.O_ID), Bytes.toBytes(Long.parseLong(oid))));
        keyValueList.add(new KeyValue(rowkey, Bytes.toBytes(Constants.DEFAULT_CF), Bytes.toBytes(Constants.T_ID), Bytes.toBytes(tid)));
        keyValueList.add(new KeyValue(rowkey, Bytes.toBytes(Constants.DEFAULT_CF), Bytes.toBytes(Constants.GEOM_X), xEncoding));
        keyValueList.add(new KeyValue(rowkey, Bytes.toBytes(Constants.DEFAULT_CF), Bytes.toBytes(Constants.GEOM_Y), yEncoding));
        keyValueList.add(new KeyValue(rowkey, Bytes.toBytes(Constants.DEFAULT_CF), Bytes.toBytes(Constants.GEOM_Z), zEncoding));
        keyValueList.add(new KeyValue(rowkey, Bytes.toBytes(Constants.DEFAULT_CF), Bytes.toBytes(Constants.START_POINT), Bytes.toBytes(writer.write(geoJTS.getGeometryN(0)))));
        keyValueList.add(new KeyValue(rowkey, Bytes.toBytes(Constants.DEFAULT_CF), Bytes.toBytes(Constants.END_POINT), Bytes.toBytes(writer.write(geoJTS.getGeometryN(geoJTS.getNumPoints() - 1)))));
        keyValueList.add(new KeyValue(rowkey, Bytes.toBytes(Constants.DEFAULT_CF), Bytes.toBytes(Constants.START_TIME), Bytes.toBytes((long) geo.getPoint(0).getZ())));
        keyValueList.add(new KeyValue(rowkey, Bytes.toBytes(Constants.DEFAULT_CF), Bytes.toBytes(Constants.END_TIME), Bytes.toBytes((long) geo.getPoint(geo.getPointCount() - 1).getZ())));
        keyValueList.add(new KeyValue(rowkey, Bytes.toBytes(Constants.DEFAULT_CF), Bytes.toBytes(Constants.PIVOT_POINT), Bytes.toBytes(trajectory.getDPFeature().getIndexes().stream().map(Object::toString).collect(Collectors.joining(",")))));
        keyValueList.add(new KeyValue(rowkey, Bytes.toBytes(Constants.DEFAULT_CF), Bytes.toBytes(Constants.PIVOT_MBR), Bytes.toBytes(trajectory.getDPFeature().getMBRs().toText())));
    }
}
