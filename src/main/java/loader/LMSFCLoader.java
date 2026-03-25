package loader;

import com.esri.core.geometry.*;
import config.TableConfig;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import scala.Tuple3;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static utils.TrajPutUtil.getSpatialIndex;
import utils.TrajPutUtilWithMaxsfc;

/**
 * LMSFC索引加载器
 * 
 * 使用LMSFC (Linearized Multi-Scale Space-Filling Curve) 索引策略存储轨迹数据
 * 
 * 核心特点：
 * 1. 使用统一的getSpatialIndex方法计算索引
 * 2. LMSFC返回的索引格式：(level, minSFC, maxSFC)
 * 3. 使用minSFC作为RowKey存储到HBase
 * 4. 不使用Redis统计表
 * 5. 不涉及形状编码和TSP优化
 * 
 * @author hehuajun
 */
public class LMSFCLoader extends Loader {

    /**
     * 构造函数
     * 
     * @param config 表配置
     * @param tableName 表名
     * @param sourcePath 源数据路径
     * @param resultPath 结果输出路径
     * @throws IOException IO异常
     */
    public LMSFCLoader(TableConfig config, String tableName, String sourcePath, String resultPath) 
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
    //TODO:LMSFC相关设置实现
    public LMSFCLoader(TableConfig config, String sourcePath, String resultPath) throws IOException {
        super(config, config.getTableName(), sourcePath, resultPath);
        config.setIsLMSFC(1);
        config.setThetaConfig(config.getThetaConfig());
        //validateConfig(config);
        
    }
    
    /**
     * 验证配置
     * 确保LMSFC相关配置正确
     */
    /*private void validateConfig(TableConfig config) {
        if (!config.isLMSFC()) {
            System.err.println("[LMSFCLoader] 警告: isLMSFC标志未设置，将自动设置为1");
            config.setIsLMSFC(1);
        }
        
        String thetaConfig = config.getThetaConfig();
        if (thetaConfig == null || thetaConfig.isEmpty()) {
            System.err.println("[LMSFCLoader] 警告: θ参数配置为空，将使用默认配置");
        }
        
        System.out.println("[LMSFCLoader] 配置验证完成");
        System.out.println("[LMSFCLoader] θ参数配置: " + thetaConfig);
        if (config.getEnvelope() != null) {
            Envelope env = config.getEnvelope();
            System.out.println(String.format("[LMSFCLoader] 空间边界: x[%.2f, %.2f], y[%.2f, %.2f]",
                    env.getXMin(), env.getXMax(), env.getYMin(), env.getYMax()));
        }
    }*/

    /**
     * 存储主表数据（优化版本）
     * 使用LMSFC索引策略
     * 
     * 优化点：
     * 1. 增加Spark并行度配置
     * 2. 使用mapPartitions批量处理减少函数调用开销
     * 3. 优化Kryo序列化配置
     * 4. 调整分区数以提高并行度
     * 5. 移除不必要的空Map创建
     * 
     * @throws IOException 存储过程中的IO异常
     */
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
            
            System.out.println("[LMSFCLoader] 开始构建LMSFC索引...");
            
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
                            // 对于LMSFC，返回格式为：(level, minSFC, maxSFC)
                            Tuple3<Object, Object, Object> indexValue = getSpatialIndex(rawTraj, config, 4);
                            
                            // 提取minSFC和maxSFC
                            long minSFC = (long) indexValue._2();
                            long maxSFC = (long) indexValue._3();
                            
                            // 使用TrajPutUtilWithMaxsfc构造Put（包含maxSFC列）
                            Tuple3<Put, Long, List<KeyValue>> putWithIndex = 
                                TrajPutUtilWithMaxsfc.getPutWithIndex(rawTraj, minSFC, maxSFC, config);
                            
                            results.add(new Tuple3<>(putWithIndex._1(), emptyMap, emptyList));
                            processedCount++;
                        } catch (Exception e) {
                            failedCount++;
                            System.err.println("[LMSFCLoader] 处理轨迹失败 [" + failedCount + "]: " + rawTraj);
                            System.err.println("[LMSFCLoader] 错误详情: " + e.getMessage());
                            // 重新抛出异常以确保数据完整性
                            throw new RuntimeException("轨迹索引构建失败，停止处理以避免数据丢失", e);
                        }
                    }
                    
                    System.out.println("[LMSFCLoader] 分区处理完成: 成功=" + processedCount + ", 失败=" + failedCount);
                    
                    return results.iterator();
                }).repartition(400);  // 增加分区数以提高并行度

            storePrimaryTableWithHadoopDataset(indexedRDD);

            currentTime = System.currentTimeMillis() - currentTime;
            
            // 写入索引时间
            String path = resultPath + "indexing_time_" + tableName;
            System.out.println("[LMSFCLoader] 索引时间文件路径: " + path);
            FileWriter writer = new FileWriter(path);
            writer.write("indexing time: " + currentTime);
            writer.flush();
            writer.close();
            
            System.out.println("[LMSFCLoader] 主表存储完成，耗时: " + currentTime + " ms");
        }
    }
}
