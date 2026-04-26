package experiments.suite.comparison.config;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class ComparisonDataset {
    private String name;
    private String queryBaseDir;
    private String dataFilePath;
    private Integer resolution;
    private Integer minTraj;
    private Integer alpha;
    private Integer beta;
    private Integer letiAlpha;
    private Integer letiBeta;
    private Integer nodes;
    private Integer shards;
    private String queryType;
    private String thetaConfig;
    private String thetaConfigPath;
    private String bmtreeConfigPath;
    private String bmtreeBitLength;
}
