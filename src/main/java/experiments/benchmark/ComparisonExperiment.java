package experiments.benchmark;

import config.TableConfig;
import experiments.benchmark.config.DatasetConfig;
import experiments.benchmark.config.IndexMethod;
import experiments.benchmark.io.TableBuilder;
import experiments.benchmark.model.ExperimentStats;
import lombok.Setter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import utils.LetiOrderResolver;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

/**
 * 对比实验框架主类
 * <p>
 * 运行完整实验流程:
 * 1. 加载所有数据集配置
 * 2. 对每个方法和数据集组合运行基准测试
 * 3. 汇总结果并生成对比表格
 */
public class ComparisonExperiment {
    
    private final String outputDir;
    private final List<IndexMethod> methods;
    private final List<DatasetConfig> datasets;
    private final Set<String> createdTables;
    @Setter
    private String datasetName;
    
    public ComparisonExperiment(String outputDir) {
        this.outputDir = outputDir;
        this.methods = new ArrayList<>();
        this.datasets = new ArrayList<>();
        this.createdTables = new HashSet<>();
    }

    public void addMethod(IndexMethod method) {
        methods.add(method);
    }
    
    /**
     * 添加数据集配置
     */
    public void addDataset(DatasetConfig dataset) {
        datasets.add(dataset);
    }
    
    /**
     * 创建标准实验配置
     * 为每个配置设置查询路径和数据路径
     */
    public void setupStandardExperiment(String datasetName, String queryBaseDir, String dataBaseDir) {
        // 为每个数据分布和查询范围组合创建配置
        for (String distribution : DatasetConfig.DISTRIBUTIONS) {
            for (int range : DatasetConfig.QUERY_RANGES) {
                DatasetConfig config = new DatasetConfig(datasetName, distribution, range);
                config.setQueryBaseDir(queryBaseDir);
                config.setDataBaseDir(dataBaseDir);
                addDataset(config);
            }
        }
    }
    
    /**
     * 运行完整实验
     */
    public void runExperiment() {
        BenchmarkRunner runner = new BenchmarkRunner(outputDir, true);
        List<ExperimentStats> allStats = new ArrayList<>();
        
        System.out.println("\n" + repeatString());
        System.out.println("Starting Comparison Experiment");
        System.out.println("Methods: " + methods.size() + ", Datasets: " + datasets.size());
        System.out.println(repeatString());
        
        for (IndexMethod method : methods) {
            if (method == IndexMethod.LETI) {
                Set<String> ensuredLetiTables = new HashSet<>();
                for (DatasetConfig dataset : datasets) {
                    String tableName = buildLetiTableName(method, datasetName, dataset.getDistribution());
                    if (!ensuredLetiTables.contains(tableName)) {
                        ensureTableExists(method, dataset, tableName);
                        ensuredLetiTables.add(tableName);
                    }
                    ExperimentStats stats = runner.runBenchmark(method, dataset, tableName);
                    allStats.add(stats);
                }
            } else {
                String tableName = buildTableName(method, datasetName);
                // 每个方法只检查/创建一次表
                DatasetConfig baseDataset = datasets.get(0);
                ensureTableExists(method, baseDataset, tableName);

                // 该方法的所有参数实验都共享同一个表
                for (DatasetConfig dataset : datasets) {
                    ExperimentStats stats = runner.runBenchmark(method, dataset, tableName);
                    allStats.add(stats);
                }
            }
        }
        
        // 生成对比结果
        generateComparisonTable(allStats);
        
        // 实验完成后清理表
        cleanupTables();
        
        System.out.println("\n" + repeatString());
        System.out.println("Experiment completed. Results saved to: " + outputDir);
        System.out.println(repeatString());
    }
    
    /**
     * 确保表存在，每个方法只创建一次
     *
     * @param method    索引方法
     * @param dataset   数据集配置（用于获取 resolution, alpha, beta 等参数）
     * @param tableName 表名
     */
    private void ensureTableExists(IndexMethod method, DatasetConfig dataset, String tableName) {
        try {
            Configuration conf = HBaseConfiguration.create();
            try (Connection connection = ConnectionFactory.createConnection(conf);
                 Admin admin = connection.getAdmin()) {
                
                TableName hTableName = TableName.valueOf(tableName);
                if (admin.tableExists(hTableName)) {
                    System.out.println("Table exists, deleting and recreating: " + tableName);
                    admin.disableTable(hTableName);
                    admin.deleteTable(hTableName);
                    System.out.println("Table deleted: " + tableName);
                }
                
                System.out.println("Creating table for method " + method.getShortName() + ": " + tableName);
                
                // 构建表配置 - 使用 DatasetConfig 中的参数
                TableConfig config = TableBuilder.buildConfig(dataset);

                // 为特定方法设置索引参数（从 DatasetConfig 传递到 TableConfig）
                configureIndexSpecificSettings(method, dataset, config);

                // 获取数据源路径
                String dataPath = dataset.getDataFilePath();
                
                // 创建表
                TableBuilder builder = new TableBuilder(dataPath, outputDir);
                builder.createTable(method, tableName, config);
                
                // 记录本次实验创建的表
                createdTables.add(tableName);
                
                System.out.println("Table created successfully: " + tableName);
            }
        } catch (Exception e) {
            System.err.println("Error ensuring table exists: " + tableName);
            System.err.println(e.getMessage());
        }
    }
    
    /**
     * 为特定索引方法配置参数
     * 
     * @param method  索引方法
     * @param dataset 数据集配置
     * @param config  表配置
     */
    private void configureIndexSpecificSettings(IndexMethod method, DatasetConfig dataset, TableConfig config) {
        switch (method) {
            case LETI:
                configureLETI(dataset, config);
                break;
            case LMSFC:
                configureLMSFC(dataset, config);
                break;
            case BMTREE:
                configureBMTree(dataset, config);
                break;
            default:
                // 其他方法不需要额外配置
                break;
        }
    }

    /**
     * 配置 LETI 索引参数（按数据集与分布动态选择 RL ordering 文件）。
     */
    private void configureLETI(DatasetConfig dataset, TableConfig config) {
        String orderDefinitionPath = resolveLetiOrderingPath(dataset);
        config.setAdaptivePartition(0);
        config.setOrderDefinitionPath(orderDefinitionPath);
        System.out.println("Configuring LETI index:");
        System.out.println("  - adaptivePartition: 1");
        System.out.println("  - orderDefinitionPath: " + orderDefinitionPath);
    }

    private String resolveLetiOrderingPath(DatasetConfig dataset) {
        String datasetKey = normalizeDatasetName(dataset.getDatasetName());
        String distKey = normalizeDistributionName(dataset.getDistribution());
        String candidate = String.format("leti/%s/%s_order.json", datasetKey, distKey);
        ClassLoader cl = ComparisonExperiment.class.getClassLoader();
        if (cl.getResource(candidate) != null) {
            return LetiOrderResolver.resolveClasspathResource(candidate);
        }
        return LetiOrderResolver.defaultOrderPath();
    }

    private String normalizeDatasetName(String datasetName) {
        if (datasetName == null) {
            return "unknown";
        }
        String normalized = datasetName.trim().toLowerCase(Locale.ROOT);
        return normalized.replace('-', '_');
    }

    private String normalizeDistributionName(String distribution) {
        if (distribution == null) {
            return "default";
        }
        String dist = distribution.trim().toLowerCase(Locale.ROOT);
        switch (dist) {
            case "uniform":
                return "uni";
            case "gaussian":
                return "gauss";
            case "skewed":
                return "skew";
            default:
                return dist;
        }
    }

    /**
     * 配置 LMSFC 索引参数
     */
    private void configureLMSFC(DatasetConfig dataset, TableConfig config) {
        // 获取 θ 参数配置（如果 dataset 中没有设置，使用默认值）
        String thetaConfig = dataset.getThetaConfigOrDefault();

        System.out.println("Configuring LMSFC index:");
        System.out.println("  - thetaConfig: " + thetaConfig);

        config.setIsLMSFC(1);
        config.setThetaConfig(thetaConfig);
    }

    /**
     * 配置 BMTree 索引参数
     */
    private void configureBMTree(DatasetConfig dataset, TableConfig config) {
        // 获取 BMTree 配置（如果 dataset 中没有设置，使用默认值）
        String bmtreeConfigPath = dataset.getBMTreeConfigPathOrDefault();
        String bmtreeBitLength = dataset.getBMTreeBitLengthOrDefault();

        System.out.println("Configuring BMTree index:");
        System.out.println("  - configPath: " + bmtreeConfigPath);
        System.out.println("  - bitLength: " + bmtreeBitLength);

        config.setIsBMTree(1);
        config.setBMTreeConfigPath(bmtreeConfigPath);
        config.setBMTreeBitLength(bmtreeBitLength);
    }

    /**
     * 实验完成后清理本次实验创建的表
     */
    private void cleanupTables() {
        if (createdTables.isEmpty()) {
            return;
        }
        
        System.out.println("\n" + repeatString());
        System.out.println("Cleaning up created tables...");
        
        try {
            Configuration conf = HBaseConfiguration.create();
            try (Connection connection = ConnectionFactory.createConnection(conf);
                 Admin admin = connection.getAdmin()) {
                
                for (String tableName : createdTables) {
                    TableName hTableName = TableName.valueOf(tableName);
                    if (admin.tableExists(hTableName)) {
                        try {
                            admin.disableTable(hTableName);
                            admin.deleteTable(hTableName);
                            System.out.println("Deleted table: " + tableName);
                        } catch (Exception e) {
                            System.err.println("Error deleting table " + tableName + ": " + e.getMessage());
                        }
                    }
                }
            }
        } catch (Exception e) {
            System.err.println("Error during table cleanup: " + e.getMessage());
        }
        
        System.out.println("Table cleanup completed");
        System.out.println(repeatString());
    }
    
    /**
     * 重复字符串n次
     */
    private static String repeatString() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 70; i++) {
            sb.append("=");
        }
        return sb.toString();
    }
    
    /**
     * 构建表名
     */
    private String buildTableName(IndexMethod method, String datasetName) {
        // 表名格式: <dataset>_<method>
        // 例如: tdrive_leti
        return String.format("%s_%s",
            datasetName.toLowerCase(),
            method.getShortName().toLowerCase()
        );
    }

    private String buildLetiTableName(IndexMethod method, String datasetName, String distribution) {
        String dist = distribution == null ? "default" : distribution.toLowerCase(Locale.ROOT);
        return String.format("%s_%s_%s",
                datasetName.toLowerCase(Locale.ROOT),
                method.getShortName().toLowerCase(Locale.ROOT),
                dist);
    }
    
    /**
     * 生成CSV格式的对比表格
     */
    private void generateComparisonTable(List<ExperimentStats> allStats) {
        // Output split by method groups; each file contains only the required metrics (avg only).
        String commonPath = outputDir + "/comparison_common_all_methods.csv";
        String locsPath = outputDir + "/comparison_locsindex_only.csv";
        String tspPath = outputDir + "/comparison_tspencoding_only.csv";

        try {
            Files.createDirectories(Paths.get(outputDir));

            try (BufferedWriter commonWriter = new BufferedWriter(new FileWriter(commonPath));
                 BufferedWriter locsWriter = new BufferedWriter(new FileWriter(locsPath));
                 BufferedWriter tspWriter = new BufferedWriter(new FileWriter(tspPath))) {

                commonWriter.write(getCommonHeaderAvgOnly());
                commonWriter.newLine();

                locsWriter.write(getLocSIndexHeaderAvgOnly());
                locsWriter.newLine();

                tspWriter.write(getTspHeaderAvgOnly());
                tspWriter.newLine();

                for (ExperimentStats stats : allStats) {
                    IndexMethod method = stats.getMethod();

                    // All methods
                    commonWriter.write(toCommonCsvRowAvgOnly(stats));
                    commonWriter.newLine();

                    // LocSIndex subset
                    if (isLocSIndexMethod(method)) {
                        locsWriter.write(toLocSIndexCsvRowAvgOnly(stats));
                        locsWriter.newLine();
                    }

                    // TSPEncoding subset
                    if (isTspEncodingMethod(method)) {
                        tspWriter.write(toTspCsvRowAvgOnly(stats));
                        tspWriter.newLine();
                    }
                }
            }

            generatePerMethodDistributionFiles(allStats);

            System.out.println("\nComparison outputs saved to: " + outputDir);
            System.out.println("  - " + commonPath);
            System.out.println("  - " + locsPath);
            System.out.println("  - " + tspPath);
        } catch (IOException e) {
            System.err.println("Error writing comparison outputs: " + e.getMessage());
        }
    }

    private void generatePerMethodDistributionFiles(List<ExperimentStats> allStats) throws IOException {
        Map<String, List<ExperimentStats>> grouped = new HashMap<>();
        for (ExperimentStats stats : allStats) {
            String key = buildMethodDistributionKey(stats);
            grouped.computeIfAbsent(key, k -> new ArrayList<>()).add(stats);
        }

        for (Map.Entry<String, List<ExperimentStats>> entry : grouped.entrySet()) {
            String filePath = outputDir + "/" + entry.getKey() + ".csv";
            List<ExperimentStats> groupStats = entry.getValue();
            groupStats.sort(Comparator.comparingInt(a -> a.getDataset().getQueryRange()));

            try (BufferedWriter writer = new BufferedWriter(new FileWriter(filePath))) {
                writer.write(getMethodDistributionHeader());
                writer.newLine();
                for (ExperimentStats stats : groupStats) {
                    writer.write(toMethodDistributionCsvRow(stats));
                    writer.newLine();
                }
            }
        }
    }

    private String buildMethodDistributionKey(ExperimentStats stats) {
        return String.format("%s_%s-%s",
                stats.getMethod().getShortName(),
                stats.getDataset().getDatasetName(),
                stats.getDataset().getDistribution());
    }

    private String getMethodDistributionHeader() {
        return "Method,Dataset,Distribution,QueryRange_Meters,"
                + "Latency_Avg,LogicalIndexRanges_Avg,Candidates_Avg,FinalResultCount_Avg,"
                + "QuadCodeRanges_Avg,QOrderRanges_Avg,RowKeyRanges_Avg,VisitedCells_Avg,"
                + "RedisAccessCount_Avg,RedisShapeFilterRate_Avg,IndexSize_KB";
    }

    private String toMethodDistributionCsvRow(ExperimentStats stats) {
        return String.format(Locale.US,
                "%s,%s,%s,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d",
                stats.getMethod().getShortName(),
                stats.getDataset().getDatasetName(),
                stats.getDataset().getDistribution(),
                stats.getDataset().getQueryRange(),
                stats.getLatencyStats().getAvg(),
                stats.getLogicIndexRangeStats().getAvg(),
                stats.getCandidatesStats().getAvg(),
                stats.getFinalResultStats().getAvg(),
                stats.getQuadCodeRangeStats().getAvg(),
                stats.getQOrderRangeStats().getAvg(),
                stats.getRowKeyRangeStats().getAvg(),
                stats.getVisitedCellsStats().getAvg(),
                stats.getRedisAccessCountStats().getAvg(),
                stats.getRedisShapeFilterRateScaledStats().getAvg(),
                stats.getIndexSizeKb());
    }

    private boolean isLocSIndexMethod(IndexMethod method) {
        return method == IndexMethod.LETI || method == IndexMethod.TSHAPE || method == IndexMethod.XZ_STAR;
    }

    private boolean isTspEncodingMethod(IndexMethod method) {
        return method == IndexMethod.LETI || method == IndexMethod.TSHAPE;
    }

    private String getCommonHeaderAvgOnly() {
        return "Method,Dataset,Distribution,QueryRange_Meters,"
                + "Latency_Avg,LogicalIndexRanges_Avg,VisitedCells_Avg,RowKeyRanges_Avg,"
                + "Candidates_Avg,FinalResultCount_Avg,IndexSize_KB";
    }

    private String getLocSIndexHeaderAvgOnly() {
        return "Method,Dataset,Distribution,QueryRange_Meters,"
                + "LogicalIndexRanges_Avg,QuadCodeRanges_Avg,QOrderRanges_Avg,RowKeyRanges_Avg,VisitedCells_Avg";
    }

    private String getTspHeaderAvgOnly() {
        return "Method,Dataset,Distribution,QueryRange_Meters,"
                + "RedisAccessCount_Avg,RedisShapeFilterRate_Avg";
    }

    private String toCommonCsvRowAvgOnly(ExperimentStats stats) {
        return String.format(Locale.US,
                "%s,%s,%s,%d,%d,%d,%d,%d,%d,%d,%d",
                stats.getMethod().getShortName(),
                stats.getDataset().getDatasetName(),
                stats.getDataset().getDistribution(),
                stats.getDataset().getQueryRange(),
                stats.getLatencyStats().getAvg(),
                stats.getLogicIndexRangeStats().getAvg(),
                stats.getVisitedCellsStats().getAvg(),
                stats.getRowKeyRangeStats().getAvg(),
                stats.getCandidatesStats().getAvg(),
                stats.getFinalResultStats().getAvg(),
                stats.getIndexSizeKb());
    }

    private String toLocSIndexCsvRowAvgOnly(ExperimentStats stats) {
        return String.format(Locale.US,
                "%s,%s,%s,%d,%d,%d,%d,%d,%d",
                stats.getMethod().getShortName(),
                stats.getDataset().getDatasetName(),
                stats.getDataset().getDistribution(),
                stats.getDataset().getQueryRange(),
                stats.getLogicIndexRangeStats().getAvg(),
                stats.getQuadCodeRangeStats().getAvg(),
                stats.getQOrderRangeStats().getAvg(),
                stats.getRowKeyRangeStats().getAvg(),
                stats.getVisitedCellsStats().getAvg());
    }

    private String toTspCsvRowAvgOnly(ExperimentStats stats) {
        return String.format(Locale.US,
                "%s,%s,%s,%d,%d,%d",
                stats.getMethod().getShortName(),
                stats.getDataset().getDatasetName(),
                stats.getDataset().getDistribution(),
                stats.getDataset().getQueryRange(),
                stats.getRedisAccessCountStats().getAvg(),
                stats.getRedisShapeFilterRateScaledStats().getAvg());
    }
    
    /**
     * 主入口
     * 参数: <query_base_dir> <data_base_dir> <output_dir> <dataset_name>
     * 示例: data/tquery data/tdrive results/ Tdrive
     */
    public static void main(String[] args) {
        if (args.length < 4) {
            System.out.println("Usage: ComparisonExperiment <query_base_dir> <data_base_dir> <output_dir> <dataset_name>");
            System.out.println("Example: ComparisonExperiment data/tquery data/tdrive results/ Tdrive");
            return;
        }
        
        String queryBaseDir = args[0];
        String dataBaseDir = args[1];
        String outputDir = args[2];
        String datasetName = args[3];
        
        ComparisonExperiment exp = new ComparisonExperiment(outputDir);
        
        // 添加要对比的方法
        exp.addMethod(IndexMethod.LETI);
        exp.addMethod(IndexMethod.TSHAPE);
        exp.addMethod(IndexMethod.XZ_STAR);
        exp.addMethod(IndexMethod.LMSFC);
        exp.addMethod(IndexMethod.BMTREE);
        
        // 设置标准实验配置
        exp.setDatasetName(datasetName);
        exp.setupStandardExperiment(datasetName, queryBaseDir, dataBaseDir);
        
        // 运行实验
        exp.runExperiment();
    }
}
