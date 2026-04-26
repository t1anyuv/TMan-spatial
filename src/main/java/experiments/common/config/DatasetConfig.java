package experiments.common.config;

import lombok.Getter;
import lombok.Setter;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

/**
 * 数据集配置
 */
@Getter
@Setter
public class DatasetConfig {
    public static final String T_DRIVE = "Tdrive";
    public static final String CD_TAXI = "CD-Taxi";
    public static final String UNIFORM = "uniform";
    public static final String SKEWED = "skewed";
    public static final String GAUSSIAN = "gaussian";
    public static final String RANGE = "range";
    public static final List<Integer> QUERY_RANGES = Arrays.asList(100, 500, 1000, 1500, 2000);
    public static final List<String> DISTRIBUTIONS = Arrays.asList(SKEWED, GAUSSIAN, UNIFORM);
    public static final List<Integer> NODES = Arrays.asList(2, 4, 6, 8);

    private String datasetName = T_DRIVE;
    private String distribution = SKEWED;
    private int queryRange = 1000;
    private String queryType = "SRQ";
    private int nodes = 4;
    private int resolution = 8;
    private int alpha = 3;
    private int beta = 3;
    private Integer letiAlpha = 2;
    private Integer letiBeta = 2;
    private int minTraj = 4;
    private int shards = 4;

    @Getter
    @Setter
    private String queryBaseDir;
    @Getter
    @Setter
    private String dataFilePath;
    @Getter
    @Setter
    private String thetaConfig;
    @Getter
    @Setter
    private String thetaConfigPath;
    @Getter
    @Setter
    private String bmtreeConfigPath;
    @Getter
    @Setter
    private String bmtreeBitLength;

    public DatasetConfig() {
    }

    public DatasetConfig(String datasetName) {
        this.datasetName = datasetName;
    }

    public DatasetConfig(String datasetName, String distribution, int queryRange) {
        this.datasetName = datasetName;
        this.distribution = distribution;
        this.queryRange = queryRange;
    }

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

    public String getQueryFilePath() {
        return String.format("%s/%s/queries_test.json", queryBaseDir, distribution);
    }

    public String getRangeQueryFilePath() {
        return String.format("%s/range/%s/%s_%dm.txt", queryBaseDir, distribution, distribution, queryRange);
    }

    @Override
    public String toString() {
        return String.format("%s-%s-%dm", datasetName, distribution, queryRange);
    }

    public String getThetaConfigOrDefault() {
        if (thetaConfigPath != null && !thetaConfigPath.isEmpty()) {
            try {
                return new String(Files.readAllBytes(Paths.get(thetaConfigPath)), StandardCharsets.UTF_8).trim();
            } catch (IOException e) {
                throw new IllegalStateException("Failed to read theta config file: " + thetaConfigPath, e);
            }
        }
        if (thetaConfig == null || thetaConfig.isEmpty()) {
            return "0, 1, 2, 3, 5, 7, 8, 9, 10, 11, 18, 19, 21, " +
                    "24, 25, 26, 28, 29, 31, 36, 4, 6, 12, 13, " +
                    "14, 15, 16, 17, 20, 22, 23, 27, 30, 32, " +
                    "33, 34, 35, 37, 38, 39";
        }
        return thetaConfig;
    }

    public String getBMTreeConfigPathOrDefault() {
        if (bmtreeConfigPath == null || bmtreeConfigPath.isEmpty()) {
            if (datasetName.equalsIgnoreCase(T_DRIVE)) {
                return "bmtree/tdrive/bmtree.txt";
            } else if (datasetName.equalsIgnoreCase(CD_TAXI)) {
                return "bmtree/cdtaxi/cdtaxi_bmtree.txt";
            } else {
                return "bmtree/tdrive/bmtree.txt";
            }
        }
        return bmtreeConfigPath;
    }

    public String getBMTreeBitLengthOrDefault() {
        if (bmtreeBitLength == null || bmtreeBitLength.isEmpty()) {
            return "20,20";
        }
        return bmtreeBitLength;
    }

    public int getLetiAlphaOrDefault() {
        return letiAlpha == null || letiAlpha <= 0 ? alpha : letiAlpha;
    }

    public int getLetiBetaOrDefault() {
        return letiBeta == null || letiBeta <= 0 ? beta : letiBeta;
    }

    public boolean hasLetiCustomPartition() {
        return (letiAlpha != null && letiAlpha > 0) || (letiBeta != null && letiBeta > 0);
    }
}
