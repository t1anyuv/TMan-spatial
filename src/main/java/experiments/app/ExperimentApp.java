package experiments.app;

import experiments.common.ExperimentDefaults;
import experiments.suite.ablation.AblationExperimentRunner;
import experiments.suite.bmtree.BMTreeLearningExperimentRunner;
import experiments.suite.comparison.ComparisonRunner;
import experiments.suite.comparison.config.ComparisonConfig;
import experiments.suite.lmsfc.LMSFCLearningExperimentRunner;
import experiments.suite.parameter.ParameterExperimentRunner;
import experiments.suite.similarity.SimilarityExperimentRunner;
import experiments.suite.similarity.config.SimilarityExperimentConfig;

import java.nio.file.Path;

public class ExperimentApp {
    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            printUsage();
            return;
        }

        String mode = args[0].trim().toLowerCase();
        switch (mode) {
            case "comparison":
                runComparison(args);
                break;
            case "parameter":
                runParameter(args);
                break;
            case "ablation":
                runAblation(args);
                break;
            case "similarity-query":
                runSimilarityQuery(args);
                break;
            case "lmsfc-learn":
                runLMSFCLearn(args);
                break;
            case "bmtree-learn":
                runBMTreeLearn(args);
                break;
            default:
                System.out.println("Unknown mode: " + args[0]);
                printUsage();
                break;
        }
    }

    private static void runComparison(String[] args) throws Exception {
        Path configPath = ExperimentDefaults.comparisonConfigPath(args, 1);
        ComparisonConfig config = ComparisonConfig.load(configPath);
        new ComparisonRunner(config).run();
    }

    private static void runParameter(String[] args) throws Exception {
        if (args.length < 4) {
            System.out.println("Usage: ExperimentApp parameter <query_base_dir> <data_file_path> <output_dir> [dataset_name]");
            return;
        }
        String datasetName = args.length > 4 ? args[4] : null;
        new ParameterExperimentRunner(args[1], args[2], args[3], datasetName).run();
    }

    private static void runAblation(String[] args) throws Exception {
        if (args.length < 4) {
            System.out.println("Usage: ExperimentApp ablation <query_base_dir> <data_file_path> <output_dir>");
            return;
        }
        new AblationExperimentRunner(args[1], args[2], args[3]).run();
    }

    private static void runSimilarityQuery(String[] args) throws Exception {
        Path configPath = ExperimentDefaults.comparisonConfigPath(args, 1);
        SimilarityExperimentConfig config = SimilarityExperimentConfig.load(configPath);
        new SimilarityExperimentRunner(config).run();
    }

    private static void runLMSFCLearn(String[] args) throws Exception {
        if (args.length < 5) {
            System.out.println("Usage: ExperimentApp lmsfc-learn <sample_data_path> <theta_output_path> <order_output_path> [base_order_path] [theta_bit_num] [candidate_count] [point_sample_limit] [seed]");
            return;
        }
        LMSFCLearningExperimentRunner.fromArgs(args).run();
    }

    private static void runBMTreeLearn(String[] args) throws Exception {
        if (args.length < 5) {
            System.out.println("Usage: ExperimentApp bmtree-learn <sample_data_path> <query_workload_path> <config_output_path> <order_output_path> [base_order_path] [bit_length] [max_depth] [trajectory_sample_limit] [query_sample_limit] [page_size] [min_node_size] [seed]");
            return;
        }
        BMTreeLearningExperimentRunner.fromArgs(args).run();
    }

    private static void printUsage() {
        System.out.println("Usage:");
        System.out.println("  ExperimentApp comparison [config_json_path]");
        System.out.println("  ExperimentApp parameter <query_base_dir> <data_file_path> <output_dir> [dataset_name]");
        System.out.println("  ExperimentApp ablation <query_base_dir> <data_file_path> <output_dir>");
        System.out.println("  ExperimentApp similarity-query [config_json_path]");
        System.out.println("  ExperimentApp lmsfc-learn <sample_data_path> <theta_output_path> <order_output_path> [base_order_path] [theta_bit_num] [candidate_count] [point_sample_limit] [seed]");
        System.out.println("  ExperimentApp bmtree-learn <sample_data_path> <query_workload_path> <config_output_path> <order_output_path> [base_order_path] [bit_length] [max_depth] [trajectory_sample_limit] [query_sample_limit] [page_size] [min_node_size] [seed]");
    }
}
