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

import java.io.Closeable;
import java.io.IOException;

import static client.Constants.*;

/**
 * 查询工具类
 * 用于从 HBase 元数据表读取表配置信息
 */
public class QueryUtils implements Cloneable, Closeable {
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
        int orderEncodingType = 0;
        try {
            byte[] orderEncodingBytes = result.getValue(Bytes.toBytes(DEFAULT_CF), Bytes.toBytes(META_TABLE_ORDER_ENCODING_TYPE));
            if (orderEncodingBytes != null) {
                orderEncodingType = Bytes.toInt(orderEncodingBytes);
            }
        } catch (Exception ignored) {
        }
        int adaptivePartition = 0;
        try {
            adaptivePartition = Bytes.toInt(result.getValue(Bytes.toBytes(DEFAULT_CF), Bytes.toBytes(META_TABLE_ADAPTIVE_PARTITION)));
        } catch (Exception ignored) {
        }
        int mainTableMoveBits = 0;
        try {
            mainTableMoveBits = Bytes.toInt(result.getValue(Bytes.toBytes(DEFAULT_CF), Bytes.toBytes(META_TABLE_MAIN_TABLE_MOVE_BITS)));
        } catch (Exception ignored) {
        }
        String letiOrderName = readString(result, META_TABLE_LETI_ORDER_NAME);
        String letiOrderDataset = readString(result, META_TABLE_LETI_ORDER_DATASET);
        String letiOrderDistribution = readString(result, META_TABLE_LETI_ORDER_DISTRIBUTION);
        String letiOrderVersion = readString(result, META_TABLE_LETI_ORDER_VERSION);
        long letiOrderCount = readLong(result, META_TABLE_LETI_ORDER_COUNT);
        long letiActiveCells = readLong(result, META_TABLE_LETI_ACTIVE_CELLS);
        long letiTotalCells = readLong(result, META_TABLE_LETI_TOTAL_CELLS);
        int letiMaxLevel = readInt(result, META_TABLE_LETI_MAX_LEVEL);
        int letiGlobalAlpha = readInt(result, META_TABLE_LETI_GLOBAL_ALPHA);
        int letiGlobalBeta = readInt(result, META_TABLE_LETI_GLOBAL_BETA);
        Envelope letiOrderBoundary = readEnvelope(result,
                META_TABLE_LETI_BOUNDARY_XMIN,
                META_TABLE_LETI_BOUNDARY_YMIN,
                META_TABLE_LETI_BOUNDARY_XMAX,
                META_TABLE_LETI_BOUNDARY_YMAX);
        int isLMSFC = 0;
        try {
            isLMSFC = Bytes.toInt(result.getValue(Bytes.toBytes(DEFAULT_CF), Bytes.toBytes(META_TABLE_IS_LMSFC)));
        } catch (Exception ignored) {
        }
        int isBMTree = 0;
        try{
            isBMTree = Bytes.toInt(result.getValue(Bytes.toBytes(DEFAULT_CF), Bytes.toBytes(META_TABLE_IS_BMTREE)));
        } catch (Exception ignored) {
        }
        String bmtreeConfigPath = null;
        try {
            byte[] pathBytes = result.getValue(Bytes.toBytes(DEFAULT_CF), Bytes.toBytes(META_TABLE_BMTREE_CONFIG_PATH));
            if (pathBytes != null) {
                bmtreeConfigPath = Bytes.toString(pathBytes);
            }
        } catch (Exception ignored) {
        }
        String bmtreeBitLength = null;
        try {
            byte[] bitLengthBytes = result.getValue(Bytes.toBytes(DEFAULT_CF),
                    Bytes.toBytes(META_TABLE_BMTREE_BIT_LENGTH));
            if (bitLengthBytes != null) {
                bmtreeBitLength = Bytes.toString(bitLengthBytes);
            }
        } catch (Exception ignored) {
        }
        String thetaConfig = null;
        try {
            byte[] thetaConfigBytes = result.getValue(Bytes.toBytes(DEFAULT_CF), Bytes.toBytes(META_TABLE_THETA_CONFIG));
            if (thetaConfigBytes != null) {
                thetaConfig = Bytes.toString(thetaConfigBytes);
            }
            //thetaConfig = Bytes.toInt(result.getValue(Bytes.toBytes(DEFAULT_CF), Bytes.toBytes(META_TABLE_THETA_CONFIG));
        } catch (Exception ignored) {
        }
        String redisHost = Bytes.toString(result.getValue(Bytes.toBytes(DEFAULT_CF), Bytes.toBytes(META_REDIS)));
        String orderDefinitionPath = null;
        try {
            byte[] orderDefinitionPathBytes = result.getValue(Bytes.toBytes(DEFAULT_CF), Bytes.toBytes(META_TABLE_ORDER_DEFINITION_PATH));
            if (orderDefinitionPathBytes != null) {
                orderDefinitionPath = Bytes.toString(orderDefinitionPathBytes);
            }
        } catch (Exception ignored) {
        }
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
        tableConfig.setOrderEncodingType(orderEncodingType);
        tableConfig.setTspEncoding(tspEncoding);
        tableConfig.setOrderDefinitionPath(orderDefinitionPath);
        tableConfig.setAdaptivePartition(adaptivePartition);
        tableConfig.setMainTableMoveBits(mainTableMoveBits);
        tableConfig.setLetiOrderName(letiOrderName);
        tableConfig.setLetiOrderDataset(letiOrderDataset);
        tableConfig.setLetiOrderDistribution(letiOrderDistribution);
        tableConfig.setLetiOrderVersion(letiOrderVersion);
        tableConfig.setLetiOrderCount(letiOrderCount);
        tableConfig.setLetiActiveCells(letiActiveCells);
        tableConfig.setLetiTotalCells(letiTotalCells);
        tableConfig.setLetiMaxLevel(letiMaxLevel);
        tableConfig.setLetiGlobalAlpha(letiGlobalAlpha);
        tableConfig.setLetiGlobalBeta(letiGlobalBeta);
        tableConfig.setLetiOrderBoundary(letiOrderBoundary);
        tableConfig.setIsLMSFC(isLMSFC);
        tableConfig.setThetaConfig(thetaConfig);
        tableConfig.setIsBMTree(isBMTree);
        tableConfig.setBMTreeConfigPath(bmtreeConfigPath);
        tableConfig.setBMTreeBitLength(bmtreeBitLength);
        return tableConfig;
    }

    private static String readString(Result result, String qualifier) {
        try {
            byte[] value = result.getValue(Bytes.toBytes(DEFAULT_CF), Bytes.toBytes(qualifier));
            return value == null ? null : Bytes.toString(value);
        } catch (Exception ignored) {
            return null;
        }
    }

    private static int readInt(Result result, String qualifier) {
        try {
            byte[] value = result.getValue(Bytes.toBytes(DEFAULT_CF), Bytes.toBytes(qualifier));
            return value == null ? 0 : Bytes.toInt(value);
        } catch (Exception ignored) {
            return 0;
        }
    }

    private static long readLong(Result result, String qualifier) {
        try {
            byte[] value = result.getValue(Bytes.toBytes(DEFAULT_CF), Bytes.toBytes(qualifier));
            return value == null ? 0L : Bytes.toLong(value);
        } catch (Exception ignored) {
            return 0L;
        }
    }

    private static Envelope readEnvelope(Result result, String xminKey, String yminKey, String xmaxKey, String ymaxKey) {
        try {
            byte[] xminBytes = result.getValue(Bytes.toBytes(DEFAULT_CF), Bytes.toBytes(xminKey));
            byte[] yminBytes = result.getValue(Bytes.toBytes(DEFAULT_CF), Bytes.toBytes(yminKey));
            byte[] xmaxBytes = result.getValue(Bytes.toBytes(DEFAULT_CF), Bytes.toBytes(xmaxKey));
            byte[] ymaxBytes = result.getValue(Bytes.toBytes(DEFAULT_CF), Bytes.toBytes(ymaxKey));
            if (xminBytes == null || yminBytes == null || xmaxBytes == null || ymaxBytes == null) {
                return null;
            }
            return new Envelope(
                    Bytes.toDouble(xminBytes),
                    Bytes.toDouble(yminBytes),
                    Bytes.toDouble(xmaxBytes),
                    Bytes.toDouble(ymaxBytes)
            );
        } catch (Exception ignored) {
            return null;
        }
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

    @Override
    public void close() throws IOException {
        connection.close();
    }
}
