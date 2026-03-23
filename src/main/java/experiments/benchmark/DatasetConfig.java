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
}
