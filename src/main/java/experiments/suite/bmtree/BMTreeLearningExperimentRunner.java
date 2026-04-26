package experiments.suite.bmtree;

import utils.BMTreeConfigLearner;
import utils.BMTreeOrderFileBuilder;

import java.io.IOException;

/**
 * Experiment entry for trajectory-and-workload-driven BMTree learning and LETI-order generation.
 * <p>
 * Recommended command order:
 * bmtree-learn <sample_data_path> <query_workload_path> <config_output_path> <order_output_path>
 *              [base_order_path] [bit_length] [max_depth]
 *              [trajectory_sample_limit] [query_sample_limit] [page_size] [min_node_size] [seed]
 */
public class BMTreeLearningExperimentRunner {

    private final String sampleDataPath;
    private final String queryWorkloadPath;
    private final String baseOrderPath;
    private final String configOutputPath;
    private final String orderOutputPath;
    private final int[] bitLength;
    private final int maxDepth;
    private final int trajectorySampleLimit;
    private final int querySampleLimit;
    private final int pageSize;
    private final int minNodeSize;
    private final long seed;

    public BMTreeLearningExperimentRunner(String sampleDataPath,
                                          String queryWorkloadPath,
                                          String baseOrderPath,
                                          String configOutputPath,
                                          String orderOutputPath,
                                          int[] bitLength,
                                          int maxDepth,
                                          int trajectorySampleLimit,
                                          int querySampleLimit,
                                          int pageSize,
                                          int minNodeSize,
                                          long seed) {
        this.sampleDataPath = sampleDataPath;
        this.queryWorkloadPath = queryWorkloadPath;
        this.baseOrderPath = baseOrderPath;
        this.configOutputPath = configOutputPath;
        this.orderOutputPath = orderOutputPath;
        this.bitLength = bitLength;
        this.maxDepth = maxDepth;
        this.trajectorySampleLimit = trajectorySampleLimit;
        this.querySampleLimit = querySampleLimit;
        this.pageSize = pageSize;
        this.minNodeSize = minNodeSize;
        this.seed = seed;
    }

    public void run() throws IOException {
        BMTreeOrderFileBuilder.ensureLearnedOrderExists(
                baseOrderPath,
                configOutputPath,
                orderOutputPath,
                bitLength,
                maxDepth,
                sampleDataPath,
                queryWorkloadPath,
                trajectorySampleLimit,
                querySampleLimit,
                pageSize,
                minNodeSize,
                seed
        );
        String resourceOrderPath = BMTreeOrderFileBuilder.ensureLearnedOrderResource(
                baseOrderPath,
                bitLength,
                maxDepth,
                sampleDataPath,
                queryWorkloadPath,
                trajectorySampleLimit,
                querySampleLimit,
                pageSize,
                minNodeSize,
                seed
        );

        System.out.println("BMTree learning finished");
        System.out.println("sample input : " + sampleDataPath);
        System.out.println("query input  : " + queryWorkloadPath);
        System.out.println("base order   : " + baseOrderPath);
        System.out.println("config output: " + configOutputPath);
        System.out.println("order output : " + orderOutputPath);
        System.out.println("resource path: " + resourceOrderPath);
        System.out.println("bit length   : " + formatBitLength(bitLength));
        System.out.println("max depth    : " + maxDepth);
        System.out.println("traj limit   : " + trajectorySampleLimit);
        System.out.println("query limit  : " + querySampleLimit);
        System.out.println("page size    : " + pageSize);
        System.out.println("min node size: " + minNodeSize);
        System.out.println("seed         : " + seed);
    }

    public static BMTreeLearningExperimentRunner fromArgs(String[] args) {
        if (args.length < 5) {
            throw new IllegalArgumentException(
                    "Usage: ExperimentApp bmtree-learn <sample_data_path> <query_workload_path> <config_output_path> " +
                            "<order_output_path> [base_order_path] [bit_length] [max_depth] " +
                            "[trajectory_sample_limit] [query_sample_limit] [page_size] [min_node_size] [seed]");
        }

        String sampleDataPath = args[1].trim();
        String queryWorkloadPath = args[2].trim();
        String configOutputPath = args[3].trim();
        String orderOutputPath = args[4].trim();
        String baseOrderPath = args.length >= 6 ? args[5].trim() : BMTreeOrderFileBuilder.DEFAULT_BASE_ORDER;
        int[] bitLength = args.length >= 7 ? parseBitLength(args[6].trim()) : new int[]{20, 20};
        int maxDepth = args.length >= 8 ? Integer.parseInt(args[7].trim()) : 8;
        int trajectorySampleLimit = args.length >= 9
                ? Integer.parseInt(args[8].trim())
                : BMTreeConfigLearner.DEFAULT_TRAJECTORY_SAMPLE_LIMIT;
        int querySampleLimit = args.length >= 10
                ? Integer.parseInt(args[9].trim())
                : BMTreeConfigLearner.DEFAULT_QUERY_SAMPLE_LIMIT;
        int pageSize = args.length >= 11 ? Integer.parseInt(args[10].trim()) : BMTreeConfigLearner.DEFAULT_PAGE_SIZE;
        int minNodeSize = args.length >= 12 ? Integer.parseInt(args[11].trim()) : BMTreeConfigLearner.DEFAULT_MIN_NODE_SIZE;
        long seed = args.length >= 13 ? Long.parseLong(args[12].trim()) : BMTreeConfigLearner.DEFAULT_SEED;

        return new BMTreeLearningExperimentRunner(
                sampleDataPath,
                queryWorkloadPath,
                baseOrderPath,
                configOutputPath,
                orderOutputPath,
                bitLength,
                maxDepth,
                trajectorySampleLimit,
                querySampleLimit,
                pageSize,
                minNodeSize,
                seed
        );
    }

    private static int[] parseBitLength(String text) {
        String[] parts = text.split(",");
        int[] bitLength = new int[parts.length];
        for (int i = 0; i < parts.length; i++) {
            bitLength[i] = Integer.parseInt(parts[i].trim());
        }
        return bitLength;
    }

    private static String formatBitLength(int[] bitLength) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < bitLength.length; i++) {
            if (i > 0) {
                sb.append(',');
            }
            sb.append(bitLength[i]);
        }
        return sb.toString();
    }
}
