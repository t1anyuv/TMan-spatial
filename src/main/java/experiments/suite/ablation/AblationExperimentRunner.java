package experiments.suite.ablation;

import config.TableConfig;
import experiments.common.ExperimentDefaults;
import experiments.common.config.DatasetConfig;
import experiments.common.config.IndexMethod;
import experiments.common.execution.PlannerBackedQueryExecutor;
import experiments.common.io.BenchmarkTableCleaner;
import experiments.common.io.QueryLoader;
import experiments.common.io.TableBuilder;
import experiments.common.model.QueryMetrics;
import experiments.standalone.query.BasicQuery;
import experiments.suite.ablation.model.AblationResultRow;
import experiments.suite.ablation.output.AblationSummaryWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import utils.BMTreeConfigLearner;
import utils.BMTreeOrderFileBuilder;
import utils.LetiOrderResolver;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

public class AblationExperimentRunner {
    private final String queryBaseDir;
    private final String dataFilePath;
    private final String outputDir;
    private final PlannerBackedQueryExecutor queryExecutor = new PlannerBackedQueryExecutor();
    private final Set<String> createdTables = new LinkedHashSet<>();

    public AblationExperimentRunner(String queryBaseDir, String dataFilePath, String outputDir) {
        this.queryBaseDir = queryBaseDir;
        this.dataFilePath = dataFilePath;
        this.outputDir = outputDir;
    }

    public void run() throws IOException {
        List<AblationResultRow> rows = new ArrayList<>();
        try {
            for (Variant variant : variants()) {
                rows.add(runVariant(variant));
            }
        } finally {
            cleanupTables();
        }

        Path outputFile = Paths.get(outputDir, "ablation_summary.csv");
        AblationSummaryWriter.write(outputFile, rows);
        System.out.println("Ablation summary written to: " + outputFile);
    }

    private AblationResultRow runVariant(Variant variant) throws IOException {
        DatasetConfig datasetConfig = baseDatasetConfig();
        String orderPath = resolveOrderPath(variant, datasetConfig);
        String tableName = buildTableName(variant);
        TableConfig tableConfig = buildTableConfig(datasetConfig, variant, orderPath);
        ensureTable(tableName, datasetConfig, tableConfig);

        BasicQuery queryStrategy = PlannerBackedQueryExecutor.createQueryStrategy(tableConfig);
        List<QueryLoader.QueryWindow> queries = new QueryLoader().loadRangeQueries(datasetConfig);
        if (queries.isEmpty()) {
            throw new IllegalStateException("No queries found for ablation experiment");
        }

        queryExecutor.warmup(queries.get(0).toCsvString(), tableName, tableConfig, queryStrategy);

        long latencySum = 0L;
        long rkiSum = 0L;
        long ctSum = 0L;
        long finalSum = 0L;
        int countedQueries = 0;
        for (int i = 1; i < queries.size(); i++) {
            QueryMetrics metrics = queryExecutor.execute(queries.get(i).toCsvString(), tableName, tableConfig, queryStrategy);
            latencySum += metrics.getLatencyMs();
            rkiSum += metrics.getRki();
            ctSum += metrics.getCt();
            finalSum += metrics.getFinalCount();
            countedQueries++;
        }

        if (countedQueries == 0) {
            QueryMetrics metrics = queryExecutor.execute(queries.get(0).toCsvString(), tableName, tableConfig, queryStrategy);
            latencySum += metrics.getLatencyMs();
            rkiSum += metrics.getRki();
            ctSum += metrics.getCt();
            finalSum += metrics.getFinalCount();
            countedQueries = 1;
        }

        return new AblationResultRow(
                variant.label,
                variant.orderSource,
                variant.adaptivePartition ? 1 : 0,
                datasetConfig.getResolution(),
                datasetConfig.getMinTraj(),
                ((double) latencySum) / countedQueries,
                ((double) rkiSum) / countedQueries,
                ((double) ctSum) / countedQueries,
                ((double) finalSum) / countedQueries,
                countedQueries);
    }

    private DatasetConfig baseDatasetConfig() {
        DatasetConfig datasetConfig = new DatasetConfig(
                detectDatasetName(),
                ExperimentDefaults.DEFAULT_DISTRIBUTION,
                ExperimentDefaults.DEFAULT_QUERY_RANGE);
        datasetConfig.setQueryBaseDir(queryBaseDir);
        datasetConfig.setDataFilePath(dataFilePath);
        datasetConfig.setResolution(ExperimentDefaults.DEFAULT_RESOLUTION);
        datasetConfig.setMinTraj(ExperimentDefaults.DEFAULT_MIN_TRAJ);
        datasetConfig.setAlpha(ExperimentDefaults.DEFAULT_ALPHA);
        datasetConfig.setBeta(ExperimentDefaults.DEFAULT_BETA);
        datasetConfig.setLetiAlpha(ExperimentDefaults.DEFAULT_ALPHA);
        datasetConfig.setLetiBeta(ExperimentDefaults.DEFAULT_BETA);
        datasetConfig.setNodes(ExperimentDefaults.DEFAULT_NODES);
        datasetConfig.setQueryType(ExperimentDefaults.DEFAULT_QUERY_TYPE);
        return datasetConfig;
    }

    private List<Variant> variants() {
        return java.util.Arrays.asList(
                new Variant("LETI", true, "LETI"),
                new Variant("LETI (LMSFC)", true, "LMSFC"),
                new Variant("LETI (BMT)", true, "BMT"),
                new Variant("LETI-w/o-ASP", false, "LETI")
        );
    }

    private String resolveOrderPath(Variant variant, DatasetConfig datasetConfig) throws IOException {
        String datasetKey = datasetResourceKey(datasetConfig.getDatasetName());
        int resolution = datasetConfig.getResolution();
        int minTraj = datasetConfig.getMinTraj();
        int letiAlpha = datasetConfig.getLetiAlphaOrDefault();
        int letiBeta = datasetConfig.getLetiBetaOrDefault();
        String distribution = datasetConfig.getDistribution();
        String resource;
        switch (variant.orderSource) {
            case "LMSFC":
                resource = String.format(Locale.ROOT, "lmsfc/%s/%s_r%d_min%d_a%d_b%d.json",
                        datasetKey,
                        distribution,
                        resolution,
                        minTraj,
                        letiAlpha,
                        letiBeta);
                break;
            case "BMT":
                return ensureLearnedBMTreeOrder(datasetConfig);
            case "LETI":
            default:
                resource = String.format(Locale.ROOT, "leti/%s/param/%s_r%d_min%d_a%d_b%d.json",
                        datasetKey,
                        distribution,
                        resolution,
                        minTraj,
                        letiAlpha,
                        letiBeta);
                break;
        }
        if (AblationExperimentRunner.class.getClassLoader().getResource(resource) == null) {
            throw new IllegalStateException("Missing ablation order resource: " + resource);
        }
        return LetiOrderResolver.resolveClasspathResource(resource);
    }

    private String ensureLearnedBMTreeOrder(DatasetConfig datasetConfig) throws IOException {
        String datasetKey = datasetResourceKey(datasetConfig.getDatasetName());
        String baseOrderPath = String.format(Locale.ROOT, "leti/%s/param/%s_r%d_min%d_a%d_b%d.json",
                datasetKey,
                datasetConfig.getDistribution(),
                datasetConfig.getResolution(),
                datasetConfig.getMinTraj(),
                datasetConfig.getLetiAlphaOrDefault(),
                datasetConfig.getLetiBetaOrDefault());
        return BMTreeOrderFileBuilder.ensureLearnedOrderResource(
                baseOrderPath,
                parseBitLength(datasetConfig.getBMTreeBitLengthOrDefault()),
                datasetConfig.getResolution(),
                datasetConfig.getDataFilePath(),
                datasetConfig.getRangeQueryFilePath(),
                BMTreeConfigLearner.DEFAULT_TRAJECTORY_SAMPLE_LIMIT,
                BMTreeConfigLearner.DEFAULT_QUERY_SAMPLE_LIMIT,
                BMTreeConfigLearner.DEFAULT_PAGE_SIZE,
                BMTreeConfigLearner.DEFAULT_MIN_NODE_SIZE,
                BMTreeConfigLearner.DEFAULT_SEED);
    }

    private String datasetResourceKey(String datasetName) {
        if (datasetName == null) {
            return "tdrive";
        }
        String normalized = datasetName.trim().toLowerCase(Locale.ROOT).replace("-", "").replace("_", "");
        if ("cdtaxi".equals(normalized)) {
            return "cd_taxi";
        }
        return "tdrive";
    }

    private String detectDatasetName() {
        String normalized = (dataFilePath + " " + queryBaseDir).toLowerCase(Locale.ROOT).replace('\\', '/');
        if (normalized.contains("cdtaxi") || normalized.contains("cd_taxi") || normalized.contains("chengdu")) {
            return DatasetConfig.CD_TAXI;
        }
        return DatasetConfig.T_DRIVE;
    }

    private int[] parseBitLength(String bitLengthText) {
        String[] parts = bitLengthText.split(",");
        int[] bitLength = new int[parts.length];
        for (int i = 0; i < parts.length; i++) {
            bitLength[i] = Integer.parseInt(parts[i].trim());
        }
        return bitLength;
    }

    private TableConfig buildTableConfig(DatasetConfig datasetConfig, Variant variant, String orderPath) {
        TableConfig tableConfig = TableBuilder.buildConfig(datasetConfig);
        tableConfig.setIsXZ(0);
        tableConfig.setOrderEncodingType(1);
        tableConfig.setTspEncoding(1);
        tableConfig.setAdaptivePartition(variant.adaptivePartition ? 1 : 0);
        tableConfig.setAlpha(datasetConfig.getLetiAlphaOrDefault());
        tableConfig.setBeta(datasetConfig.getLetiBetaOrDefault());
        tableConfig.setOrderDefinitionPath(orderPath);
        return tableConfig;
    }

    private void ensureTable(String tableName, DatasetConfig datasetConfig, TableConfig tableConfig) {
        if (createdTables.contains(tableName)) {
            return;
        }
        try {
            Configuration conf = HBaseConfiguration.create();
            try (Connection connection = ConnectionFactory.createConnection(conf);
                 Admin admin = connection.getAdmin()) {
                TableName hTableName = TableName.valueOf(tableName);
                if (admin.tableExists(hTableName)) {
                    BenchmarkTableCleaner.deleteTableArtifacts(admin, tableName, "127.0.0.1");
                }
                TableBuilder builder = new TableBuilder(
                        datasetConfig.getDataFilePath(),
                        outputDir,
                        datasetConfig.getRangeQueryFilePath());
                builder.createTable(IndexMethod.LETI, tableName, tableConfig);
                createdTables.add(tableName);
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to prepare ablation table " + tableName, e);
        }
    }

    private void cleanupTables() {
        if (createdTables.isEmpty()) {
            return;
        }
        try {
            Configuration conf = HBaseConfiguration.create();
            try (Connection connection = ConnectionFactory.createConnection(conf);
                 Admin admin = connection.getAdmin()) {
                for (String tableName : createdTables) {
                    BenchmarkTableCleaner.deleteTableArtifacts(admin, tableName, "127.0.0.1");
                }
            }
        } catch (Exception e) {
            System.err.println("Error cleaning ablation experiment tables: " + e.getMessage());
        }
    }

    private String buildTableName(Variant variant) {
        return String.format(Locale.ROOT, "%s_ablation_%s",
                datasetResourceKey(detectDatasetName()),
                variant.label.toLowerCase(Locale.ROOT).replaceAll("[^a-z0-9]+", "_"));
    }

    private static class Variant {
        final String label;
        final boolean adaptivePartition;
        final String orderSource;

        private Variant(String label, boolean adaptivePartition, String orderSource) {
            this.label = label;
            this.adaptivePartition = adaptivePartition;
            this.orderSource = orderSource;
        }
    }
}
