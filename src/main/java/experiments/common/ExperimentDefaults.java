package experiments.common;

import experiments.common.config.DatasetConfig;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

public final class ExperimentDefaults {
    public static final String DEFAULT_COMPARISON_CONFIG = "src/main/resources/benchmark/comparison-config.json";

    public static final String TDRIVE_DATASET = DatasetConfig.T_DRIVE;
    public static final String DEFAULT_DISTRIBUTION = DatasetConfig.SKEWED;
    public static final int DEFAULT_QUERY_RANGE = 1000;

    public static final int DEFAULT_RESOLUTION = 8;
    public static final int DEFAULT_MIN_TRAJ = 4;
    public static final int DEFAULT_ALPHA = 3;
    public static final int DEFAULT_BETA = 3;
    public static final int DEFAULT_NODES = 4;
    public static final String DEFAULT_QUERY_TYPE = "SRQ";

    public static final List<Integer> PARAMETER_RESOLUTION_VALUES = Arrays.asList(6, 7, 8, 9, 10);
    public static final List<Integer> PARAMETER_MIN_TRAJ_VALUES = Arrays.asList(2, 4, 6, 8, 10);

    private ExperimentDefaults() {
    }

    public static Path comparisonConfigPath(String[] args, int index) {
        if (args.length > index) {
            return Paths.get(args[index]);
        }
        return Paths.get(DEFAULT_COMPARISON_CONFIG);
    }
}
