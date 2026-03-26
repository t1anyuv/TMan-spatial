package experiments.benchmark;

import lombok.Getter;
import lombok.Setter;

import java.util.Arrays;
import java.util.List;

/**
 * 数据集配置
 */
@Getter
public class DatasetConfig {
    // 数据集名称
    public static final String T_DRIVE = "Tdrive";
    public static final String CD_TAXI = "CD-Taxi";

    // 数据分布类型
    public static final String UNIFORM = "uniform";
    public static final String SKEWED = "skewed";
    public static final String GAUSSIAN = "gaussian";
    public static final String RANGE = "range";

    // 查询范围（米）
    public static final List<Integer> QUERY_RANGES = Arrays.asList(100, 500, 1000, 1500, 2000);

    // 所有数据分布类型
    public static final List<String> DISTRIBUTIONS = Arrays.asList(SKEWED, GAUSSIAN, UNIFORM);

    public static final List<Integer> NODES = Arrays.asList(2, 4, 6, 8);

    private String datasetName = T_DRIVE;   // Tdrive 或 CD-Taxi
    private String distribution = SKEWED;  // uniform, skewed, gaussian
    private int queryRange = 500;         // 100, 500, 1000, 1500, 2000
    private String queryType = "SRQ";    // SRQ, SS, kNN, SRQ-k
    private int nodes = 4;              // 2, 4, 6, 8
    private int resolution = 8;
    private int alpha = 3;
    private int beta = 3;
    private int shards = 4;

    // 路径配置
    @Getter @Setter
    private String queryBaseDir;   // 查询数据所在目录，如 data/tquery
    @Getter @Setter
    private String dataBaseDir;    // 轨迹数据所在目录，如 data/tdrive

    // LMSFC 索引配置
    @Getter
    @Setter
    private String thetaConfig; // θ参数配置，例如 "0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19"

    // BMTree 索引配置
    @Getter
    @Setter
    private String bmtreeConfigPath; // BMTree配置文件路径，例如 "bmtree/tdrive_bmtree.txt"
    @Getter
    @Setter
    private String bmtreeBitLength; // bit长度配置，例如 "20,20"


    public DatasetConfig() {
        // 使用默认值
    }

    public DatasetConfig(String datasetName) {
        this.datasetName = datasetName;
    }

    public DatasetConfig(String datasetName, String distribution, int queryRange) {
        this.datasetName = datasetName;
        this.distribution = distribution;
        this.queryRange = queryRange;
    }

    /**
     * 创建配置
     */
    public DatasetConfig(String datasetName, String distribution, int queryRange,
                         String queryType, int nodes, int resolution, int alpha, int beta, int shards) {
        this.datasetName = datasetName;
        this.distribution = distribution;
        this.queryRange = queryRange;
        this.queryType = queryType;
        this.nodes = nodes;
        this.resolution = resolution;
        this.alpha = alpha;
        this.beta = beta;
        this.shards = shards;
    }


    /**
     * 获取查询文件路径
     * 使用传入的queryBaseDir，例如: data/tquery/uniform/queries_test.json
     */
    public String getQueryFilePath() {
        return String.format("%s/%s/queries_test.json",
                queryBaseDir, distribution);
    }

    /**
     * 获取范围查询文件路径
     * 使用传入的queryBaseDir，例如: data/tquery/range/uniform/uniform_100m.txt
     */
    public String getRangeQueryFilePath() {
        return String.format("%s/range/%s/%s_%dm.txt",
                queryBaseDir, distribution, distribution, queryRange);
    }

    /**
     * 获取原始数据文件路径
     * 使用传入的dataBaseDir，例如: data/tdrive/
     */
    public String getDataFilePath() {
        return dataBaseDir;
    }

    @Override
    public String toString() {
        return String.format("%s-%s-%dm", datasetName, distribution, queryRange);
    }

    /**
     * 获取 LMSFC 的 θ 参数配置
     * 如果未设置，返回默认配置
     */
    public String getThetaConfigOrDefault() {
        if (thetaConfig == null || thetaConfig.isEmpty()) {
            // 默认使用前20个层级
            return "0, 1, 2, 3, 5, 7, 8, 9, 10, 11, 18, 19, 21, 24, 25, 26, 28, 29, 31, 36, 4, 6, 12, 13, 14, 15, 16, 17, 20, 22, 23, 27, 30, 32, 33, 34, 35, 37, 38, 39";
        }
        return thetaConfig;
    }

    /**
     * 获取 BMTree 配置文件路径
     * 如果未设置，根据数据集名称返回默认路径
     */
    public String getBMTreeConfigPathOrDefault() {
        if (bmtreeConfigPath == null || bmtreeConfigPath.isEmpty()) {
            // 根据数据集名称返回默认路径
            if (datasetName.equalsIgnoreCase(T_DRIVE)) {
                return "bmtree/tdrive_bmtree.txt";
            } else if (datasetName.equalsIgnoreCase(CD_TAXI)) {
                return "bmtree/cd_bmtree.txt";
            } else {
                return "bmtree/tdrive_bmtree.txt";
            }
        }
        return bmtreeConfigPath;
    }

    /**
     * 获取 BMTree bit长度配置
     * 如果未设置，返回默认配置
     */
    public String getBMTreeBitLengthOrDefault() {
        if (bmtreeBitLength == null || bmtreeBitLength.isEmpty()) {
            // 默认配置：2维，每维20bit
            return "20,20";
        }
        return bmtreeBitLength;
    }










}
