package utils;

import client.Constants;
import config.TableConfig;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.locationtech.jts.io.ParseException;
import scala.Tuple2;
import scala.Tuple3;

import java.io.Serializable;
import java.util.List;

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
public class TrajPutUtilWithMaxSFC extends TrajPutUtil implements Serializable {


    /**
     * 构造 Put 操作（LMSFC 专用，包含 maxSFC）
     * <p>
     * 使用 LMSFC 索引值构造 RowKey 并生成 Put 操作。
     * RowKey 格式：[minSFC(8字节)][tid]，可选分片前缀
     * <p>
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

        Tuple3<Put, Long, List<KeyValue>> base = TrajPutUtil.getPutWithIndex(traj, new Tuple2<>(minSFC, rowkey), config);
        appendMaxSFC(base._1(), base._3(), maxSFC);
        return base;
    }

    /**
     * 直接计算 LMSFC 索引并构造 Put 操作。
     *
     * @param traj 轨迹字符串（格式：oid-tid-wkt）
     * @param config 表配置（必须启用 LMSFC）
     * @return Tuple2<Put, Long> Put 操作和 minSFC 索引值
     * @throws ParseException WKT 解析异常
     */
    public static Tuple2<Put, Long> getPut(String traj, TableConfig config) throws ParseException {
        Tuple3<Object, Object, Object> spatialIndex = TrajPutUtil.getSpatialIndex(traj, config, 4);
        long minSFC = (long) spatialIndex._2();
        long maxSFC = (long) spatialIndex._3();
        Tuple3<Put, Long, List<KeyValue>> base = getPutWithIndex(traj, minSFC, maxSFC, config);
        return new Tuple2<>(base._1(), base._2());
    }

    private static void appendMaxSFC(Put put, List<KeyValue> keyValueList, long maxSFC) {
        byte[] cf = Bytes.toBytes(Constants.DEFAULT_CF);
        byte[] qualifier = Bytes.toBytes(Constants.MAX_SFC);
        byte[] value = Bytes.toBytes(maxSFC);
        put.addColumn(cf, qualifier, value);
        keyValueList.add(new KeyValue(put.getRow(), cf, qualifier, value));
    }

    static byte[] buildRowkey(long minSFC, String tid, TableConfig config) {
        return TrajPutUtil.buildRowkey(minSFC, tid, config);
    }
}
