package loader;

import config.TableConfig;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import scala.Tuple3;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static client.Constants.DEFAULT_CF;
import static client.Constants.MAX_SFC;
import static client.Constants.META_TABLE_BMTREE_BIT_LENGTH;
import static client.Constants.META_TABLE_BMTREE_CONFIG_PATH;
import static client.Constants.META_TABLE_IS_BMTREE;
import static utils.TrajPutUtil.getSpatialIndex;
import utils.TrajPutUtilWithMaxSFC;

public class BMTreeLoader extends Loader {

    /**
     * 构造函数
     * 
     * @param config 表配置
     * @param tableName 表名
     * @param sourcePath 源数据路径
     * @param resultPath 结果输出路径
     * @throws IOException IO异常
     */
    public BMTreeLoader(TableConfig config, String tableName, String sourcePath, String resultPath) 
            throws IOException {
        super(config, tableName, sourcePath, resultPath);
    }
    
    /**
     * 构造函数：使用配置中的表名
     * 
     * @param config 表配置
     * @param sourcePath 源数据路径
     * @param resultPath 结果输出路径
     * @throws IOException IO异常
     */
    public BMTreeLoader(TableConfig config, String sourcePath, String resultPath) throws IOException {
        super(config, config.getTableName(), sourcePath, resultPath);
        config.setIsBMTree(1);
        config.setBMTreeConfigPath(config.getBMTreeConfigPath());
        int[] bitLength = config.getBMTreeBitLength();
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < bitLength.length; i++) {
            sb.append(bitLength[i]);
            if (i < bitLength.length - 1) {
                sb.append(",");
            }
        }
        config.setBMTreeBitLength(sb.toString());
    }

    @Override
    protected void appendIndexMeta(Put put) {
        put.addColumn(Bytes.toBytes(DEFAULT_CF), Bytes.toBytes(META_TABLE_IS_BMTREE), Bytes.toBytes(config.getIsBMTree()));
        put.addColumn(Bytes.toBytes(DEFAULT_CF), Bytes.toBytes(META_TABLE_BMTREE_CONFIG_PATH), Bytes.toBytes(config.getBMTreeConfigPath()));
        int[] bitLength = config.getBMTreeBitLength();
        if (bitLength == null || bitLength.length == 0) {
            return;
        }
        StringBuilder bitLengthStr = new StringBuilder();
        for (int i = 0; i < bitLength.length; i++) {
            if (i > 0) {
                bitLengthStr.append(",");
            }
            bitLengthStr.append(bitLength[i]);
        }
        put.addColumn(Bytes.toBytes(DEFAULT_CF), Bytes.toBytes(META_TABLE_BMTREE_BIT_LENGTH), Bytes.toBytes(bitLengthStr.toString()));
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
            
            System.out.println("[BMTreeLoader] 开始构建BMTree索引...");
            
            // 使用mapPartitions批量处理，减少函数调用开销
            JavaRDD<Tuple3<Put, Map<String, Tuple2<Long, byte[]>>, List<KeyValue>>> indexedRDD = 
                rawTrajRDD.mapPartitions(partition -> {
                    List<Tuple3<Put, Map<String, Tuple2<Long, byte[]>>, List<KeyValue>>> results = 
                        new ArrayList<>();
                    
                    // 预分配空Map，避免重复创建
                    Map<String, Tuple2<Long, byte[]>> emptyMap = new HashMap<>();
                    List<KeyValue> emptyList = new ArrayList<>();
                    
                    int processedCount = 0;
                    int failedCount = 0;
                    
                    while (partition.hasNext()) {
                        String rawTraj = partition.next();
                        
                        try {
                            // 调用统一的getSpatialIndex方法
                            // 对于BMTree，返回格式为：(level, minSFC, maxSFC)
                            // type=5表示BMTree
                            Tuple3<Object, Object, Object> indexValue = getSpatialIndex(rawTraj, config, 5);
                            
                            // 提取minSFC和maxSFC
                            long minSFC = (long) indexValue._2();
                            long maxSFC = (long) indexValue._3();
                            
                            // 使用TrajPutUtilWithMaxSFC构造Put
                            Tuple3<Put, Long, List<KeyValue>> putWithIndex = 
                                TrajPutUtilWithMaxSFC.getPutWithIndex(rawTraj, minSFC, maxSFC, config);
                            
                            results.add(new Tuple3<>(putWithIndex._1(), emptyMap, emptyList));
                            processedCount++;
                        } catch (Exception e) {
                            failedCount++;
                            System.err.println("[BMTreeLoader] 处理轨迹失败 [" + failedCount + "]: " + rawTraj);
                            System.err.println("[BMTreeLoader] 错误详情: " + e.getMessage());
                            // 重新抛出异常以确保数据完整性
                            throw new RuntimeException("轨迹索引构建失败，停止处理以避免数据丢失", e);
                        }
                    }
                    
                    System.out.println("[BMTreeLoader] 分区处理完成: 成功=" + processedCount + ", 失败=" + failedCount);
                    
                    return results.iterator();
                }).repartition(400).persist(org.apache.spark.storage.StorageLevel.DISK_ONLY());  // 增加分区数以提高并行度

            long mainTableBytes = sumPrimaryTableBytes(indexedRDD);
            long extraIndexInfoBytes = sumQualifierBytes(indexedRDD, MAX_SFC);
            storePrimaryTableWithHadoopDataset(indexedRDD);

            currentTime = System.currentTimeMillis() - currentTime;
            System.out.println("[BMTreeLoader] 主表存储完成，耗时: " + currentTime + " ms");
            StoreSummary summary = new StoreSummary();
            summary.setTrajectoryCount(trajectoryCount);
            summary.setNodeCount(resolveNodeCount());
            summary.setShapeCount(NOT_APPLICABLE);
            summary.setIndexingTimeMs(currentTime);
            summary.setMainTableBytes(mainTableBytes);
            summary.setIndexTableBytes(0L);
            summary.setExtraIndexInfoBytes(extraIndexInfoBytes);
            summary.setNote("shape count is not applicable for interval-based BMTree storage");
            printAndPersistStoreSummary(summary);
            indexedRDD.unpersist(false);
        }
    }
}
