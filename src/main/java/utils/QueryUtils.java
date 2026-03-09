package utils;

import com.esri.core.geometry.Envelope;
import config.TableConfig;
import constans.IndexEnum;
import preprocess.compress.IIntegerCompress;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

import static client.Constants.*;

/**
 * 查询工具类
 * 用于从 HBase 元数据表读取表配置信息
 */
public class QueryUtils implements Cloneable {
    private final Connection connection;

    public QueryUtils() throws IOException {
        Configuration conf = HBaseConfiguration.create();
        this.connection = ConnectionFactory.createConnection(conf);
        connection.getAdmin();
    }

    public TableConfig getTableConfig(String tableName) throws IOException {
        Table hTable = this.connection.getTable(TableName.valueOf(tableName));
        Result result = hTable.get(new Get(Bytes.toBytes(META_TABLE_ROWKEY)));
        String indexName = Bytes.toString(result.getValue(Bytes.toBytes(DEFAULT_CF), Bytes.toBytes(META_TABLE_PRIMARY)));
        int alpha = Bytes.toInt(result.getValue(Bytes.toBytes(DEFAULT_CF), Bytes.toBytes(META_TABLE_ALPHA)));
        int resolution = Bytes.toInt(result.getValue(Bytes.toBytes(DEFAULT_CF), Bytes.toBytes(META_TABLE_RESOLUTION)));
        int beta = Bytes.toInt(result.getValue(Bytes.toBytes(DEFAULT_CF), Bytes.toBytes(META_TABLE_BETA)));
        double timeBin = Bytes.toDouble(result.getValue(Bytes.toBytes(DEFAULT_CF), Bytes.toBytes(META_TABLE_TIME_BIN)));
        int binNums = Bytes.toInt(result.getValue(Bytes.toBytes(DEFAULT_CF), Bytes.toBytes(META_TABLE_TIME_BIN_NUMS)));
        int isXZ = Bytes.toInt(result.getValue(Bytes.toBytes(DEFAULT_CF), Bytes.toBytes(META_TABLE_IS_XZ)));
        int tspEncoding = Bytes.toInt(result.getValue(Bytes.toBytes(DEFAULT_CF), Bytes.toBytes(META_TABLE_IS_TSP_ENCODING)));
        int isRLEncoding = 0;
        try {
            isRLEncoding = Bytes.toInt(result.getValue(Bytes.toBytes(DEFAULT_CF), Bytes.toBytes(META_TABLE_IS_RL_ENCODING)));
        } catch (Exception ignored) {
        }
        int adaptivePartition = 0;
        try {
            adaptivePartition = Bytes.toInt(result.getValue(Bytes.toBytes(DEFAULT_CF), Bytes.toBytes(META_TABLE_ADAPTIVE_PARTITION)));
        } catch (Exception ignored) {
        }
        int maxShapeBits = 0;
        try {
            maxShapeBits = Bytes.toInt(result.getValue(Bytes.toBytes(DEFAULT_CF), Bytes.toBytes(META_TABLE_MAX_SHAPE_BITS)));
        } catch (Exception ignored) {
        }
        String redisHost = Bytes.toString(result.getValue(Bytes.toBytes(DEFAULT_CF), Bytes.toBytes(META_REDIS)));
        Short shards = null;
        String compression = Bytes.toString(result.getValue(Bytes.toBytes(DEFAULT_CF), Bytes.toBytes(META_TABLE_compression)));
        Envelope envelope = null;
        try {
            double xMin = Bytes.toDouble(result.getValue(Bytes.toBytes(DEFAULT_CF), Bytes.toBytes(META_TABLE_xmin)));
            double xMax = Bytes.toDouble(result.getValue(Bytes.toBytes(DEFAULT_CF), Bytes.toBytes(META_TABLE_xmax)));
            double yMin = Bytes.toDouble(result.getValue(Bytes.toBytes(DEFAULT_CF), Bytes.toBytes(META_TABLE_ymin)));
            double yMax = Bytes.toDouble(result.getValue(Bytes.toBytes(DEFAULT_CF), Bytes.toBytes(META_TABLE_ymax)));
            if (!Double.isNaN(xMin) && !Double.isNaN(xMax) && !Double.isNaN(yMin) && !Double.isNaN(yMax)) {
                envelope = new Envelope(xMin, yMin, xMax, yMax);
            }
        } catch (Exception ignored) {
        }
        try {
            shards = Bytes.toShort(result.getValue(Bytes.toBytes(DEFAULT_CF), Bytes.toBytes(META_TABLE_SHARDS)));
        } catch (Exception ignored) {

        }
        TableConfig tableConfig = new TableConfig(IndexEnum.INDEX_TYPE.valueOf(indexName), resolution, alpha, beta, timeBin, binNums, IIntegerCompress.CompressType.valueOf(compression));
        if (null != envelope) {
            tableConfig.setEnvelope(envelope);
        }
        tableConfig.setRedisHost(redisHost);
        tableConfig.setShards(shards);
        tableConfig.setIsXZ(isXZ);
        tableConfig.setRlEncoding(isRLEncoding);
        tableConfig.setTspEncoding(tspEncoding);
        tableConfig.setAdaptivePartition(adaptivePartition);
        tableConfig.setMaxShapeBits(maxShapeBits);
        return tableConfig;
    }

    @Override
    public QueryUtils clone() {
        try {
            // TODO: copy mutable state here, so the clone can't change the internals of the original
            return (QueryUtils) super.clone();
        } catch (CloneNotSupportedException e) {
            throw new AssertionError();
        }
    }
}
