package experiments.suite.lmsfc;

import utils.LMSFCOrderFileBuilder;
import utils.LMSFCThetaLearner;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Experiment entry for LMSFC theta learning and order generation.
 * <p>
 * Recommended command order:
 * lmsfc-learn <sample_data_path> <theta_output_path> <order_output_path>
 *             [base_order_path] [theta_bit_num] [candidate_count]
 *             [point_sample_limit] [seed]
 */
public class LMSFCLearningExperimentRunner {

    private final String sampleDataPath;
    private final String thetaOutputPath;
    private final String orderOutputPath;
    private final String baseOrderPath;
    private final int candidateCount;
    private final int pointSampleLimit;
    private final long seed;
    private final int thetaBitNum;

    public LMSFCLearningExperimentRunner(String sampleDataPath,
                                         String thetaOutputPath,
                                         String orderOutputPath,
                                         String baseOrderPath,
                                         int candidateCount,
                                         int pointSampleLimit,
                                         long seed,
                                         int thetaBitNum) {
        this.sampleDataPath = sampleDataPath;
        this.thetaOutputPath = thetaOutputPath;
        this.orderOutputPath = orderOutputPath;
        this.baseOrderPath = baseOrderPath;
        this.candidateCount = candidateCount;
        this.pointSampleLimit = pointSampleLimit;
        this.seed = seed;
        this.thetaBitNum = thetaBitNum;
    }

    public void run() throws IOException {
        LMSFCThetaLearner.SearchResult result = LMSFCThetaLearner.buildLearnedOrder(
                sampleDataPath,
                baseOrderPath,
                orderOutputPath,
                thetaBitNum,
                candidateCount,
                pointSampleLimit,
                seed
        );

        writeThetaFile(result.getBestThetaConfig(), thetaOutputPath);

        System.out.println("LMSFC learning finished");
        System.out.println("theta output : " + thetaOutputPath);
        System.out.println("order output : " + orderOutputPath);
        System.out.println("best theta   : " + result.getBestThetaConfig());
    }

    private static void writeThetaFile(String thetaConfig, String thetaOutputPath) throws IOException {
        Path output = Paths.get(thetaOutputPath);
        Path parent = output.getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
        Files.write(output, thetaConfig.getBytes(StandardCharsets.UTF_8));
    }

    public static LMSFCLearningExperimentRunner fromArgs(String[] args) {
        if (args.length < 5) {
            throw new IllegalArgumentException(
                    "Usage: ExperimentApp lmsfc-learn <sample_data_path> <theta_output_path> " +
                            "<order_output_path> [base_order_path] [theta_bit_num] [candidate_count] " +
                            "[point_sample_limit] [seed]");
        }

        String sampleDataPath = args[1].trim();
        String thetaOutputPath = args[2].trim();
        String orderOutputPath = args[3].trim();
        String baseOrderPath = args.length >= 5 ? args[4].trim() : LMSFCOrderFileBuilder.DEFAULT_BASE_ORDER;
        int thetaBitNum = args.length >= 6 ? Integer.parseInt(args[5].trim()) : 20;
        int candidateCount = args.length >= 7 ? Integer.parseInt(args[6].trim()) : 64;
        int pointSampleLimit = args.length >= 8 ? Integer.parseInt(args[7].trim()) : 10_000;
        long seed = args.length >= 9 ? Long.parseLong(args[8].trim()) : 20260421L;

        return new LMSFCLearningExperimentRunner(
                sampleDataPath,
                thetaOutputPath,
                orderOutputPath,
                baseOrderPath,
                candidateCount,
                pointSampleLimit,
                seed,
                thetaBitNum
        );
    }
}
