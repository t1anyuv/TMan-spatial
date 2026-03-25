package experiments.tman;

import com.esri.core.geometry.Envelope;
import config.TableConfig;
import constans.IndexEnum;
import loader.LMSFCLoader;
import preprocess.compress.IIntegerCompress;

import java.io.IOException;

/**
 * LMSFC索引存储实验程序
 * 
 * 使用LMSFC (Linearized Multi-Scale Space-Filling Curve) 索引策略存储轨迹数据
 * 
 * 命令行参数：
 * [0]  sourcePath       - 源数据路径
 * [1]  resolution       - 分辨率（四叉树最大层级）
 * [2]  alpha            - x方向网格数量（未使用，保留兼容性）
 * [3]  beta             - y方向网格数量（未使用，保留兼容性）
 * [4]  timeBin          - 时间分箱大小
 * [5]  timeBinNums      - 时间分箱数量
 * [6]  compressType     - 压缩类型（DELTA/PFOR/SIMPLE8B/VARINT）
 * [7]  xmin             - 空间边界最小x坐标
 * [8]  ymin             - 空间边界最小y坐标
 * [9]  xmax             - 空间边界最大x坐标
 * [10] ymax             - 空间边界最大y坐标
 * [11] shards           - 分片数量
 * [12] pIndex           - 主索引类型（SPATIAL）
 * [13] tableName        - HBase表名
 * [14] redisHost        - Redis主机地址
 * [15] thetaConfig      - θ参数配置字符串（逗号分隔）
 * [16] resultPath       - 结果输出路径
 * 
 * 示例：
 * java experiments.tman.LMSFCStoring \
 *   /data/trajectories.txt \
 *   12 2 2 3600.0 24 DELTA \
 *   116.0 39.5 117.0 40.5 \
 *   4 SPATIAL lmsfc_table \
 *   127.0.0.1 \
 *   "0,1,2,3,4,5,6,7,8,9,0,1,2,3,4,5,6,7,8,9" \
 *   /results/
 */
public class LMSFCStoring {
    public static void main(String[] args) throws IOException {
        if (args.length < 17) {
            printUsage();
            System.exit(1);
        }

        // 解析命令行参数
        String sourcePath = args[0];
        int resolution = Integer.parseInt(args[1]);
        int alpha = Integer.parseInt(args[2]);
        int beta = Integer.parseInt(args[3]);
        double timeBin = Double.parseDouble(args[4]);
        int timeBinNums = Integer.parseInt(args[5]);
        IIntegerCompress.CompressType compressType = IIntegerCompress.CompressType.valueOf(args[6]);
        double xmin = Double.parseDouble(args[7]);
        double ymin = Double.parseDouble(args[8]);
        double xmax = Double.parseDouble(args[9]);
        double ymax = Double.parseDouble(args[10]);
        short shards = Short.parseShort(args[11]);
        IndexEnum.INDEX_TYPE pIndex = IndexEnum.INDEX_TYPE.valueOf(args[12]);
        String tableName = args[13];
        String redisHost = args[14];
        String thetaConfig = args[15];
        String resultPath = args[16];

        // 验证主索引类型
        if (pIndex != IndexEnum.INDEX_TYPE.SPATIAL) {
            System.err.println("错误: LMSFC索引仅支持SPATIAL主索引类型");
            System.exit(1);
        }

        // 打印配置信息
        printConfiguration(sourcePath, resolution, alpha, beta, timeBin, timeBinNums,
                compressType, xmin, ymin, xmax, ymax, shards, pIndex, tableName,
                redisHost, thetaConfig, resultPath);

        // 创建表配置
        TableConfig tableConfig = new TableConfig(
                pIndex, resolution, alpha, beta, timeBin, timeBinNums, compressType,
                new Envelope(xmin, ymin, xmax, ymax), shards
        );
        tableConfig.setTableName(tableName);
        tableConfig.setRedisHost(redisHost);
        
        // 设置LMSFC特定配置
        tableConfig.setIsLMSFC(1);
        tableConfig.setThetaConfig(thetaConfig);

        System.out.println("\n================================================================================");
        System.out.println("开始LMSFC索引存储");
        System.out.println("================================================================================\n");

        // 执行存储
        try (LMSFCLoader loader = new LMSFCLoader(tableConfig, sourcePath, resultPath)) {
            loader.store();
            System.out.println("\n================================================================================");
            System.out.println("LMSFC索引存储完成");
            System.out.println("================================================================================");
        } catch (Exception e) {
            System.err.println("\n错误: LMSFC索引存储失败");
            System.exit(1);
        }
    }

    private static void printUsage() {
        System.err.println("用法: LMSFCStoring <参数列表>");
        System.err.println("\n参数:");
        System.err.println("  [0]  sourcePath    - 源数据路径");
        System.err.println("  [1]  resolution    - 分辨率");
        System.err.println("  [2]  alpha         - x方向网格数量");
        System.err.println("  [3]  beta          - y方向网格数量");
        System.err.println("  [4]  timeBin       - 时间分箱大小");
        System.err.println("  [5]  timeBinNums   - 时间分箱数量");
        System.err.println("  [6]  compressType  - 压缩类型");
        System.err.println("  [7]  xmin          - 最小x坐标");
        System.err.println("  [8]  ymin          - 最小y坐标");
        System.err.println("  [9]  xmax          - 最大x坐标");
        System.err.println("  [10] ymax          - 最大y坐标");
        System.err.println("  [11] shards        - 分片数量");
        System.err.println("  [12] pIndex        - 主索引类型");
        System.err.println("  [13] tableName     - 表名");
        System.err.println("  [14] redisHost     - Redis主机");
        System.err.println("  [15] thetaConfig   - θ参数配置");
        System.err.println("  [16] resultPath    - 结果路径");
    }

    private static void printConfiguration(String sourcePath, int resolution, int alpha, int beta,
                                          double timeBin, int timeBinNums,
                                          IIntegerCompress.CompressType compressType,
                                          double xmin, double ymin, double xmax, double ymax,
                                          short shards, IndexEnum.INDEX_TYPE pIndex,
                                          String tableName, String redisHost,
                                          String thetaConfig, String resultPath) {
        System.out.println("\n================================================================================");
        System.out.println("LMSFC索引存储配置");
        System.out.println("================================================================================");
        System.out.println("数据源配置:");
        System.out.println("  源数据路径    : " + sourcePath);
        System.out.println("  结果路径      : " + resultPath);
        System.out.println("\n索引配置:");
        System.out.println("  索引类型      : LMSFC");
        System.out.println("  主索引        : " + pIndex.getIndexName());
        System.out.println("  分辨率        : " + resolution);
        System.out.println("  θ参数配置     : " + thetaConfig);
        System.out.println("\n空间配置:");
        System.out.println("  边界范围      : [" + xmin + ", " + ymin + "] - [" + xmax + ", " + ymax + "]");
        System.out.println("  Alpha         : " + alpha + " (未使用)");
        System.out.println("  Beta          : " + beta + " (未使用)");
        System.out.println("\n时间配置:");
        System.out.println("  时间分箱      : " + timeBin);
        System.out.println("  分箱数量      : " + timeBinNums);
        System.out.println("\n存储配置:");
        System.out.println("  表名          : " + tableName);
        System.out.println("  分片数        : " + shards);
        System.out.println("  压缩类型      : " + compressType);
        System.out.println("  Redis主机     : " + redisHost);
        System.out.println("================================================================================");
    }
}
