package utils;

import com.esri.core.geometry.*;
import client.Constants;
import config.TableConfig;
import constans.IndexEnum;
import entity.Trajectory;
import preprocess.compress.IIntegerCompress;
import utils.ByteArraysWrapper;
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
 * LMSFC 专用的轨迹数据 Put 操作工具类
 * <p>
 * 扩展自 TrajPutUtil，专门处理 LMSFC 索引的存储需求：
 * - 存储 maxSFC 作为额外的列键
 * - 保持与原有 TrajPutUtil 相同的接口
 * - 适配 LMSFC 索引的三元组返回值 (level, minSFC, maxSFC)
 *
 * @author hehuajun
 */
public class TrajPutUtilWithMaxsfc implements Serializable {
    /**
     * WKT 写入器，用于将几何对象转换为 WKT 字符串
     */
    public static final WKTWriter writer = new WKTWriter(3);
    /**
     * WKT 读取器，用于解析 WKT 字符串
     */
    public static final WKTReader reader = new WKTReader();


    /**
     * 构造 Put 操作（LMSFC 专用，包含 maxSFC）
     * <p>
     * 使用 LMSFC 索引值构造 RowKey 并生成 Put 操作。
     * RowKey 格式：[minSFC(8字节)][tid]，可选分片前缀
     * 
     * 与 TrajPutUtil 的区别：
     * - 额外存储 maxSFC 列
     * - indexValue 参数包含 (minSFC, maxSFC) 信息
     *
     * @param traj 轨迹字符串（格式：oid-tid-wkt）
     * @param minSFC minSFC 索引值（用于构造 RowKey）
     * @param maxSFC maxSFC 索引值（用于存储为列）
     * @param config 表配置
     * @return Tuple3<Put, Long, List<KeyValue>> Put 操作、minSFC 索引值、KeyValue 列表
     * @throws ParseException WKT 解析异常
     */
    public static Tuple3<Put, Long, List<KeyValue>> getPutWithIndex(
            String traj, 
            long minSFC, 
            long maxSFC, 
            TableConfig config) throws ParseException {
        
        String[] t = traj.split("-");
        String tid = t[1];
        tid = tid + (int) (Math.random() * 20);
        byte[] rowkey = buildRowkey(minSFC, tid, config);
        
        return getPutWithIndexAndMaxSFC(traj, new Tuple2<>(minSFC, rowkey), maxSFC, config);
    }

    /**
     * 构造 Put 操作（核心方法，包含 maxSFC）
     * <p>
     * 将轨迹数据编码并构造 HBase Put 操作，包括：
     * - 几何数据编码（经纬度、时间）
     * - 元数据提取（起止点、起止时间、DP 特征）
     * - maxSFC 列的存储
     * - Put 和 KeyValue 的生成
     *
     * @param traj 轨迹字符串（格式：oid-tid-wkt）
     * @param indexValue Tuple2<minSFC索引值, RowKey字节数组>
     * @param maxSFC maxSFC 索引值
     * @param config 表配置
     * @return Tuple3<Put, Long, List<KeyValue>> Put 操作、minSFC 索引值、KeyValue 列表
     * @throws ParseException WKT 解析异常
     */
    private static Tuple3<Put, Long, List<KeyValue>> getPutWithIndexAndMaxSFC(
            String traj, 
            Tuple2<Long, byte[]> indexValue, 
            long maxSFC,
            TableConfig config) throws ParseException {
        
        String[] t = traj.split("-");
        MultiPoint geo = parseWKTToMultiPoint(t[2]);
        org.locationtech.jts.geom.Geometry geoJTS = reader.read(t[2]);

        // 编码几何数据
        int[] xValue = new int[geo.getPointCount()];
        int[] yValue = new int[geo.getPointCount()];
        int[] zValue = new int[geo.getPointCount()];
        encodeGeometry(geo, xValue, yValue, zValue);

        // 压缩编码
        IIntegerCompress integerCompress = IIntegerCompress.getIntegerCompress(config.getCompressType().name());
        byte[] xEncoding = integerCompress.encoding(xValue);
        byte[] yEncoding = integerCompress.encoding(yValue);
        byte[] zEncoding = integerCompress.encoding(zValue);

        byte[] rowkey = indexValue._2;
        Trajectory trajectory = new Trajectory(t[0], t[1], geoJTS);

        Put put = new Put(rowkey);
        List<KeyValue> keyValueList = new ArrayList<>();

        // 添加列到 Put 和 KeyValue（包含 maxSFC）
        addColumnsToPut(put, t[0], t[1], geo, geoJTS, trajectory, xEncoding, yEncoding, zEncoding, maxSFC);
        addColumnsToKeyValueList(keyValueList, rowkey, t[0], t[1], geo, geoJTS, trajectory, xEncoding, yEncoding, zEncoding, maxSFC);

        return new Tuple3<>(put, indexValue._1, keyValueList);
    }

    /**
     * 直接计算 LMSFC 索引并构造 Put 操作
     * <p>
     * 根据表配置自动计算 LMSFC 索引（minSFC 和 maxSFC），构造 RowKey 并生成 Put 操作。
     * 适用于不需要 KeyValue 列表的场景。
     *
     * @param traj 轨迹字符串（格式：oid-tid-wkt）
     * @param config 表配置（必须启用 LMSFC）
     * @return Tuple2<Put, Long> Put 操作和 minSFC 索引值
     * @throws ParseException WKT 解析异常
     */
    public static Tuple2<Put, Long> getPut(String traj, TableConfig config) throws ParseException {
        String[] t = traj.split("-");
        MultiPoint geo = parseWKTToMultiPoint(t[2]);
        org.locationtech.jts.geom.Geometry geoJTS = reader.read(t[2]);

        // 编码几何数据
        int[] xValue = new int[geo.getPointCount()];
        int[] yValue = new int[geo.getPointCount()];
        int[] zValue = new int[geo.getPointCount()];
        encodeGeometry(geo, xValue, yValue, zValue);

        // 压缩编码
        IIntegerCompress integerCompress = IIntegerCompress.getIntegerCompress(config.getCompressType().name());
        byte[] xEncoding = integerCompress.encoding(xValue);
        byte[] yEncoding = integerCompress.encoding(yValue);
        byte[] zEncoding = integerCompress.encoding(zValue);

        // 计算 LMSFC 索引
        Tuple3<Object, Object, Object> spatialIndex = TrajPutUtil.getSpatialIndex(traj, config, 4);
        long minSFC = (long) spatialIndex._2();
        long maxSFC = (long) spatialIndex._3();

        // 构造 RowKey
        String tid = t[1] + (int) (Math.random() * 20);
        byte[] rowkey = buildRowkey(minSFC, tid, config);

        Put put = new Put(rowkey);
        
        // 添加基础列
        put.addColumn(Bytes.toBytes(Constants.DEFAULT_CF), Bytes.toBytes(Constants.O_ID), Bytes.toBytes(Long.parseLong(t[0])));
        put.addColumn(Bytes.toBytes(Constants.DEFAULT_CF), Bytes.toBytes(Constants.T_ID), Bytes.toBytes(t[1]));
        put.addColumn(Bytes.toBytes(Constants.DEFAULT_CF), Bytes.toBytes(Constants.GEOM_X), xEncoding);
        put.addColumn(Bytes.toBytes(Constants.DEFAULT_CF), Bytes.toBytes(Constants.GEOM_Y), yEncoding);
        put.addColumn(Bytes.toBytes(Constants.DEFAULT_CF), Bytes.toBytes(Constants.GEOM_Z), zEncoding);
        
        // 添加元数据列
        Trajectory trajectory = new Trajectory(t[0], t[1], geoJTS);
        put.addColumn(Bytes.toBytes(Constants.DEFAULT_CF), Bytes.toBytes(Constants.START_POINT), 
                     Bytes.toBytes(writer.write(geoJTS.getGeometryN(0))));
        put.addColumn(Bytes.toBytes(Constants.DEFAULT_CF), Bytes.toBytes(Constants.END_POINT), 
                     Bytes.toBytes(writer.write(geoJTS.getGeometryN(geoJTS.getNumPoints() - 1))));
        put.addColumn(Bytes.toBytes(Constants.DEFAULT_CF), Bytes.toBytes(Constants.START_TIME), 
                     Bytes.toBytes((long) geo.getPoint(0).getZ()));
        put.addColumn(Bytes.toBytes(Constants.DEFAULT_CF), Bytes.toBytes(Constants.END_TIME), 
                     Bytes.toBytes((long) geo.getPoint(geo.getPointCount() - 1).getZ()));
        put.addColumn(Bytes.toBytes(Constants.DEFAULT_CF), Bytes.toBytes(Constants.PIVOT_POINT), 
                     Bytes.toBytes(trajectory.getDPFeature().getIndexes().stream().map(Object::toString).collect(Collectors.joining(","))));
        put.addColumn(Bytes.toBytes(Constants.DEFAULT_CF), Bytes.toBytes(Constants.PIVOT_MBR), 
                     Bytes.toBytes(trajectory.getDPFeature().getMBRs().toText()));
        
        // 添加 maxSFC 列（LMSFC 特有）
        put.addColumn(Bytes.toBytes(Constants.DEFAULT_CF), Bytes.toBytes(Constants.MAX_SFC), Bytes.toBytes(maxSFC));
        
        return new Tuple2<>(put, minSFC);
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
     * RowKey 格式：[minSFC(8字节)][tid]，可选分片前缀 [shard(1字节)][minSFC(8字节)][tid]
     *
     * @param minSFC minSFC 索引值
     * @param tid    轨迹ID
     * @param config 表配置
     * @return RowKey 字节数组
     */
    private static byte[] buildRowkey(long minSFC, String tid, TableConfig config) {
        byte[] tidBytes = Bytes.toBytes(tid);
        byte[] rowkeyCore = new byte[8 + tidBytes.length];
        ByteArraysWrapper.writeLong(minSFC, rowkeyCore, 0);
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
     * 添加列到 Put 对象（包含 maxSFC）
     * 
     * @param maxSFC maxSFC 索引值
     */
    private static void addColumnsToPut(Put put, String oid, String tid, MultiPoint geo,
                                        org.locationtech.jts.geom.Geometry geoJTS, Trajectory trajectory,
                                        byte[] xEncoding, byte[] yEncoding, byte[] zEncoding, long maxSFC) {
        // 基础列
        put.addColumn(Bytes.toBytes(Constants.DEFAULT_CF), Bytes.toBytes(Constants.O_ID), Bytes.toBytes(Long.parseLong(oid)));
        put.addColumn(Bytes.toBytes(Constants.DEFAULT_CF), Bytes.toBytes(Constants.T_ID), Bytes.toBytes(tid));
        put.addColumn(Bytes.toBytes(Constants.DEFAULT_CF), Bytes.toBytes(Constants.GEOM_X), xEncoding);
        put.addColumn(Bytes.toBytes(Constants.DEFAULT_CF), Bytes.toBytes(Constants.GEOM_Y), yEncoding);
        put.addColumn(Bytes.toBytes(Constants.DEFAULT_CF), Bytes.toBytes(Constants.GEOM_Z), zEncoding);
        
        // 元数据列
        put.addColumn(Bytes.toBytes(Constants.DEFAULT_CF), Bytes.toBytes(Constants.START_POINT), 
                     Bytes.toBytes(writer.write(geoJTS.getGeometryN(0))));
        put.addColumn(Bytes.toBytes(Constants.DEFAULT_CF), Bytes.toBytes(Constants.END_POINT), 
                     Bytes.toBytes(writer.write(geoJTS.getGeometryN(geoJTS.getNumPoints() - 1))));
        put.addColumn(Bytes.toBytes(Constants.DEFAULT_CF), Bytes.toBytes(Constants.START_TIME), 
                     Bytes.toBytes((long) geo.getPoint(0).getZ()));
        put.addColumn(Bytes.toBytes(Constants.DEFAULT_CF), Bytes.toBytes(Constants.END_TIME), 
                     Bytes.toBytes((long) geo.getPoint(geo.getPointCount() - 1).getZ()));
        put.addColumn(Bytes.toBytes(Constants.DEFAULT_CF), Bytes.toBytes(Constants.PIVOT_POINT), 
                     Bytes.toBytes(trajectory.getDPFeature().getIndexes().stream().map(Object::toString).collect(Collectors.joining(","))));
        put.addColumn(Bytes.toBytes(Constants.DEFAULT_CF), Bytes.toBytes(Constants.PIVOT_MBR), 
                     Bytes.toBytes(trajectory.getDPFeature().getMBRs().toText()));
        
        // LMSFC 特有：maxSFC 列
        put.addColumn(Bytes.toBytes(Constants.DEFAULT_CF), Bytes.toBytes(Constants.MAX_SFC), Bytes.toBytes(maxSFC));
    }

    /**
     * 添加列到 KeyValue 列表（包含 maxSFC）
     * 
     * @param maxSFC maxSFC 索引值
     */
    private static void addColumnsToKeyValueList(List<KeyValue> keyValueList, byte[] rowkey, String oid, String tid,
                                                 MultiPoint geo, org.locationtech.jts.geom.Geometry geoJTS,
                                                 Trajectory trajectory, byte[] xEncoding, byte[] yEncoding, 
                                                 byte[] zEncoding, long maxSFC) {
        // 基础列
        keyValueList.add(new KeyValue(rowkey, Bytes.toBytes(Constants.DEFAULT_CF), Bytes.toBytes(Constants.O_ID), 
                                      Bytes.toBytes(Long.parseLong(oid))));
        keyValueList.add(new KeyValue(rowkey, Bytes.toBytes(Constants.DEFAULT_CF), Bytes.toBytes(Constants.T_ID), 
                                      Bytes.toBytes(tid)));
        keyValueList.add(new KeyValue(rowkey, Bytes.toBytes(Constants.DEFAULT_CF), Bytes.toBytes(Constants.GEOM_X), 
                                      xEncoding));
        keyValueList.add(new KeyValue(rowkey, Bytes.toBytes(Constants.DEFAULT_CF), Bytes.toBytes(Constants.GEOM_Y), 
                                      yEncoding));
        keyValueList.add(new KeyValue(rowkey, Bytes.toBytes(Constants.DEFAULT_CF), Bytes.toBytes(Constants.GEOM_Z), 
                                      zEncoding));
        
        // 元数据列
        keyValueList.add(new KeyValue(rowkey, Bytes.toBytes(Constants.DEFAULT_CF), Bytes.toBytes(Constants.START_POINT), 
                                      Bytes.toBytes(writer.write(geoJTS.getGeometryN(0)))));
        keyValueList.add(new KeyValue(rowkey, Bytes.toBytes(Constants.DEFAULT_CF), Bytes.toBytes(Constants.END_POINT), 
                                      Bytes.toBytes(writer.write(geoJTS.getGeometryN(geoJTS.getNumPoints() - 1)))));
        keyValueList.add(new KeyValue(rowkey, Bytes.toBytes(Constants.DEFAULT_CF), Bytes.toBytes(Constants.START_TIME), 
                                      Bytes.toBytes((long) geo.getPoint(0).getZ())));
        keyValueList.add(new KeyValue(rowkey, Bytes.toBytes(Constants.DEFAULT_CF), Bytes.toBytes(Constants.END_TIME), 
                                      Bytes.toBytes((long) geo.getPoint(geo.getPointCount() - 1).getZ())));
        keyValueList.add(new KeyValue(rowkey, Bytes.toBytes(Constants.DEFAULT_CF), Bytes.toBytes(Constants.PIVOT_POINT), 
                                      Bytes.toBytes(trajectory.getDPFeature().getIndexes().stream().map(Object::toString).collect(Collectors.joining(",")))));
        keyValueList.add(new KeyValue(rowkey, Bytes.toBytes(Constants.DEFAULT_CF), Bytes.toBytes(Constants.PIVOT_MBR), 
                                      Bytes.toBytes(trajectory.getDPFeature().getMBRs().toText())));
        
        // LMSFC 特有：maxSFC 列
        keyValueList.add(new KeyValue(rowkey, Bytes.toBytes(Constants.DEFAULT_CF), Bytes.toBytes(Constants.MAX_SFC), 
                                      Bytes.toBytes(maxSFC)));
    }
}
