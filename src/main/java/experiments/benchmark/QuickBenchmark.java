package experiments.benchmark;

/**
 * 快速基准测试工具 - 用于快速测试单个配置
 * <p>
 * 用法示例:
 * java experiments.benchmark.QuickBenchmark datasets/ tdrive LETI gaussian 1000
 */
public class QuickBenchmark {
    
    private final String datasetBaseDir;
    private final String outputDir;
    
    public QuickBenchmark(String datasetBaseDir, String outputDir) {
        this.datasetBaseDir = datasetBaseDir;
        this.outputDir = outputDir;
    }
    
    /**
     * 运行快速测试
     */
    public void runQuickTest(String datasetName, IndexMethod method, 
                             String distribution, int queryRange) {
        BenchmarkRunner runner = new BenchmarkRunner(outputDir);
        DatasetConfig config = new DatasetConfig(datasetName, distribution, queryRange);
        config.setQueryBaseDir(datasetBaseDir + "/" + datasetName.toLowerCase() + "query");
        config.setDataBaseDir(datasetBaseDir + "/" + datasetName.toLowerCase());
        
        System.out.println("\n" + repeatString("="));
        System.out.println("Quick Benchmark Test");
        System.out.println(repeatString("="));
        System.out.println("Dataset: " + datasetName);
        System.out.println("Method: " + method.getShortName());
        System.out.println("Distribution: " + distribution);
        System.out.println("Query Range: " + queryRange + "m");
        System.out.println(repeatString("="));
        
        String tableName = String.format("%s_%s_%s_%dm",
            datasetName.toLowerCase(),
            method.getShortName().toLowerCase(),
            distribution,
            queryRange
        );
        
        ExperimentStats stats = runner.runBenchmark(method, config, tableName);
        
        // 打印结果
        printStats(stats);
    }
    
    private void printStats(ExperimentStats stats) {
        System.out.println("\n" + repeatString("-"));
        System.out.println("Results Summary");
        System.out.println(repeatString("-"));
        System.out.printf("Method: %s%n", stats.getMethod().getShortName());
        System.out.printf("Dataset: %s%n", stats.getDataset().toString());
        System.out.println();
        System.out.printf("Latency (ms) - Avg: %d, Min: %d, Max: %d%n",
            stats.getLatencyStats().getAvg(),
            stats.getLatencyStats().getMin(),
            stats.getLatencyStats().getMax());
        System.out.printf("Logic index ranges (SFC) - Avg: %d%n", stats.getLogicIndexRangeStats().getAvg());
        System.out.printf("Row-key scan ranges - Avg: %d%n", stats.getRowKeyRangeStats().getAvg());
        System.out.printf("Candidates - Avg: %d%n", stats.getCandidatesStats().getAvg());
        System.out.printf("VC (Visited Cells) - Avg: %.2f%n", stats.getVisitedCellsStats().getAvgDouble());
        System.out.printf("Final Size - Avg: %d, Min: %d, Max: %d%n",
            stats.getFinalResultStats().getAvg(),
            stats.getFinalResultStats().getMin(),
            stats.getFinalResultStats().getMax());
        System.out.println(repeatString("-"));
    }
    
    /**
     * 重复字符串n次
     */
    private static String repeatString(String str) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 50; i++) {
            sb.append(str);
        }
        return sb.toString();
    }
    
    public static void main(String[] args) {
        if (args.length < 5) {
            System.out.println("Usage: QuickBenchmark <dataset_base_dir> <output_dir> <dataset_name> <method> <distribution> <query_range>");
            System.out.println("Example: QuickBenchmark datasets/ results/ tdrive LETI gaussian 1000");
            System.out.println();
            System.out.println("Supported methods: LETI, TSHAPE, XZ_STAR");
            System.out.println("Supported distributions: uniform, skewed, gaussian, range");
            return;
        }
        
        String datasetBaseDir = args[0];
        String outputDir = args[1];
        String datasetName = args[2];
        String methodName = args[3].toUpperCase();
        String distribution = args[4];
        int queryRange = Integer.parseInt(args[5]);
        
        // 解析方法
        IndexMethod method;
        try {
            method = IndexMethod.valueOf(methodName);
        } catch (IllegalArgumentException e) {
            System.err.println("Unknown method: " + methodName);
            return;
        }
        
        QuickBenchmark benchmark = new QuickBenchmark(datasetBaseDir, outputDir);
        benchmark.runQuickTest(datasetName, method, distribution, queryRange);
    }
}
