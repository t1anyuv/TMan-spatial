package experiments.benchmark;

import config.TableConfig;
import experiments.benchmark.config.DatasetConfig;
import experiments.benchmark.config.IndexMethod;
import experiments.benchmark.io.TableBuilder;
import experiments.benchmark.model.ExperimentStats;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

public class ParameterExperiment {

    private static final List<Integer> RESOLUTION_VALUES = Arrays.asList(6, 7, 8, 9, 10);
    private static final List<Integer> MIN_TRAJ_VALUES = Arrays.asList(1, 2, 3, 4, 5);
    private static final List<Integer> ALPHA_BETA_VALUES = Arrays.asList(2, 3, 4, 5);
    private static final List<String> RESOLUTION_MIN_TRAJ_METRICS = Arrays.asList(
            "Latency_ms", "LogicalIndexRanges", "VisitedCells", "RowKeyRanges");
    private static final List<String> ALPHA_BETA_METRICS = Arrays.asList("IndexSize_KB");

    private final String outputDir;
    private final String queryBaseDir;
    private final String dataBaseDir;
    private final String datasetName;
    private final BenchmarkRunner runner;
    private final Set<String> createdTables = new LinkedHashSet<>();
    private final Set<String> ensuredTables = new LinkedHashSet<>();

    public ParameterExperiment(String queryBaseDir, String dataBaseDir, String outputDir, String datasetName) {
        this.queryBaseDir = queryBaseDir;
        this.dataBaseDir = dataBaseDir;
        this.outputDir = outputDir;
        this.datasetName = datasetName;
        this.runner = new BenchmarkRunner(outputDir);
    }

    public void runDefaultStudy() {
        List<ResultRow> rows = new ArrayList<>();
        try {
            Files.createDirectories(Paths.get(outputDir));
            runResolutionSweep(rows);
            runMinTrajSweep(rows);
            runAlphaBetaSweep(rows);
            writeFlatResults(rows);
            writeResolutionMinTrajTables(rows);
            writeIndexSizeTable(rows);
        } catch (IOException e) {
            throw new RuntimeException("Failed to run parameter experiment", e);
        } finally {
            cleanupTables();
        }
    }

    private void runResolutionSweep(List<ResultRow> rows) {
        DatasetConfig base = defaultConfig();
        for (int resolution : RESOLUTION_VALUES) {
            DatasetConfig config = copyConfig(base);
            config.setResolution(resolution);
            for (MethodVariant variant : standardVariants()) {
                appendMetricRows(rows, runOne(variant, config, "resolution", String.valueOf(resolution)),
                        "resolution", config.getDatasetName(), variant.label, String.valueOf(resolution),
                        RESOLUTION_MIN_TRAJ_METRICS);
            }
        }
    }

    private void runMinTrajSweep(List<ResultRow> rows) {
        DatasetConfig base = defaultConfig();
        for (int minTraj : MIN_TRAJ_VALUES) {
            DatasetConfig config = copyConfig(base);
            config.setMinTraj(minTraj);
            for (MethodVariant variant : standardVariants()) {
                appendMetricRows(rows, runOne(variant, config, "minTraj", String.valueOf(minTraj)),
                        "minTraj", config.getDatasetName(), variant.label, String.valueOf(minTraj),
                        RESOLUTION_MIN_TRAJ_METRICS);
            }
        }
    }

    private void runAlphaBetaSweep(List<ResultRow> rows) {
        DatasetConfig base = defaultConfig();
        for (int alphaBeta : ALPHA_BETA_VALUES) {
            DatasetConfig config = copyConfig(base);
            config.setAlpha(alphaBeta);
            config.setBeta(alphaBeta);
            for (MethodVariant variant : alphaBetaVariants()) {
                appendMetricRows(rows, runOne(variant, config, "alpha_beta", alphaBeta + "*" + alphaBeta),
                        "alpha_beta", config.getDatasetName(), variant.label, alphaBeta + "*" + alphaBeta,
                        ALPHA_BETA_METRICS);
            }
        }
    }

    private void appendMetricRows(List<ResultRow> rows, RunOutcome outcome, String study, String dataset,
                                  String method, String sweepValue, List<String> metrics) {
        if (outcome.isPending()) {
            addPendingRows(rows, study, dataset, method, sweepValue, metrics, outcome.pendingReason);
            return;
        }
        addRows(rows, study, dataset, method, sweepValue, outcome.stats, metrics);
    }

    private void addRows(List<ResultRow> rows, String study, String dataset, String method, String sweepValue,
                         ExperimentStats stats, List<String> metrics) {
        for (String metric : metrics) {
            rows.add(ResultRow.value(study, dataset, metric, method, sweepValue, formatMetric(metric, stats)));
        }
    }

    private void addPendingRows(List<ResultRow> rows, String study, String dataset, String method, String sweepValue,
                                List<String> metrics, String reason) {
        for (String metric : metrics) {
            rows.add(ResultRow.pending(study, dataset, metric, method, sweepValue, reason));
        }
    }

    private String formatMetric(String metric, ExperimentStats stats) {
        switch (metric) {
            case "Latency_ms":
                return String.valueOf(stats.getLatencyStats().getAvg());
            case "LogicalIndexRanges":
                return String.valueOf(stats.getLogicIndexRangeStats().getAvg());
            case "VisitedCells":
                return String.valueOf(stats.getVisitedCellsStats().getAvg());
            case "RowKeyRanges":
                return String.valueOf(stats.getRowKeyRangeStats().getAvg());
            case "IndexSize_KB":
                return String.valueOf(stats.getIndexSizeKb());
            default:
                return "";
        }
    }

    private RunOutcome runOne(MethodVariant variant, DatasetConfig dataset, String study, String sweepValue) {
        String pendingReason = variant.resolvePendingReason(dataset, study);
        if (pendingReason != null) {
            return RunOutcome.pending(pendingReason);
        }
        String tableName = buildTableName(variant, dataset, study, sweepValue);
        ensureTableExists(variant, dataset, tableName, study);
        return RunOutcome.value(runner.runBenchmark(variant.baseMethod, dataset, tableName));
    }

    private void ensureTableExists(MethodVariant variant, DatasetConfig dataset, String tableName, String study) {
        if (ensuredTables.contains(tableName)) {
            return;
        }
        try {
            Configuration conf = HBaseConfiguration.create();
            try (Connection connection = ConnectionFactory.createConnection(conf);
                 Admin admin = connection.getAdmin()) {
                TableName hTableName = TableName.valueOf(tableName);
                if (admin.tableExists(hTableName)) {
                    admin.disableTable(hTableName);
                    admin.deleteTable(hTableName);
                }

                TableConfig config = TableBuilder.buildConfig(dataset);
                variant.configure(dataset, config, study);

                TableBuilder builder = new TableBuilder(dataset.getDataFilePath(), outputDir);
                builder.createTable(variant.baseMethod, tableName, config);
                ensuredTables.add(tableName);
                createdTables.add(tableName);
            }
        } catch (Exception e) {
            throw new RuntimeException("Error creating table " + tableName, e);
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
                    TableName hTableName = TableName.valueOf(tableName);
                    if (!admin.tableExists(hTableName)) {
                        continue;
                    }
                    admin.disableTable(hTableName);
                    admin.deleteTable(hTableName);
                }
            }
        } catch (Exception e) {
            System.err.println("Error cleaning parameter experiment tables: " + e.getMessage());
        }
    }

    private void writeFlatResults(List<ResultRow> rows) throws IOException {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(Paths.get(outputDir, "parameter_results.csv").toFile()))) {
            writer.write("Study,Dataset,Metric,Method,SweepValue,Value,Status");
            writer.newLine();
            for (ResultRow row : rows) {
                writer.write(String.join(",",
                        row.study,
                        row.dataset,
                        row.metric,
                        row.method,
                        row.sweepValue,
                        row.value,
                        row.status));
                writer.newLine();
            }
        }
    }

    private void writeResolutionMinTrajTables(List<ResultRow> rows) throws IOException {
        List<String> columns = new ArrayList<>();
        for (int resolution : RESOLUTION_VALUES) {
            columns.add(String.valueOf(resolution));
        }
        for (int minTraj : MIN_TRAJ_VALUES) {
            columns.add(String.valueOf(minTraj));
        }
        for (String metric : RESOLUTION_MIN_TRAJ_METRICS) {
            Map<String, Map<String, String>> grid = new LinkedHashMap<>();
            for (MethodVariant variant : standardVariants()) {
                grid.put(variant.label, new LinkedHashMap<String, String>());
            }
            for (ResultRow row : rows) {
                if (!metric.equals(row.metric)) {
                    continue;
                }
                if (!"resolution".equals(row.study) && !"minTraj".equals(row.study)) {
                    continue;
                }
                Map<String, String> methodRow = grid.get(row.method);
                if (methodRow != null) {
                    methodRow.put(row.sweepValue, row.renderedValue());
                }
            }
            writePivot(Paths.get(outputDir, "parameter_resolution_minTraj_" + metric + ".csv").toString(),
                    columns, grid);
        }
    }

    private void writeIndexSizeTable(List<ResultRow> rows) throws IOException {
        List<String> columns = new ArrayList<>();
        for (int resolution : RESOLUTION_VALUES) {
            columns.add(String.valueOf(resolution));
        }
        for (int minTraj : MIN_TRAJ_VALUES) {
            columns.add(String.valueOf(minTraj));
        }
        for (int alphaBeta : ALPHA_BETA_VALUES) {
            columns.add(alphaBeta + "*" + alphaBeta);
        }
        Map<String, Map<String, String>> grid = new LinkedHashMap<>();
        for (MethodVariant variant : indexSizeVariants()) {
            grid.put(variant.label, new LinkedHashMap<String, String>());
        }
        for (ResultRow row : rows) {
            if (!"IndexSize_KB".equals(row.metric)) {
                continue;
            }
            if (!"resolution".equals(row.study) && !"minTraj".equals(row.study) && !"alpha_beta".equals(row.study)) {
                continue;
            }
            Map<String, String> methodRow = grid.get(row.method);
            if (methodRow != null) {
                methodRow.put(row.sweepValue, row.renderedValue());
            }
        }
        writePivot(Paths.get(outputDir, "parameter_index_size.csv").toString(), columns, grid);
    }

    private void writePivot(String filePath, List<String> columns, Map<String, Map<String, String>> grid) throws IOException {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(filePath))) {
            writer.write("Dataset/Method");
            for (String column : columns) {
                writer.write("," + column);
            }
            writer.newLine();
            for (Map.Entry<String, Map<String, String>> entry : grid.entrySet()) {
                writer.write(entry.getKey());
                for (String column : columns) {
                    writer.write("," + entry.getValue().getOrDefault(column, ""));
                }
                writer.newLine();
            }
        }
    }

    private DatasetConfig defaultConfig() {
        DatasetConfig config = new DatasetConfig(datasetName, DatasetConfig.SKEWED, 500);
        config.setQueryBaseDir(queryBaseDir);
        config.setDataBaseDir(dataBaseDir);
        config.setResolution(8);
        config.setAlpha(3);
        config.setBeta(3);
        config.setNodes(4);
        config.setQueryType("SRQ");
        return config;
    }

    private DatasetConfig copyConfig(DatasetConfig config) {
        DatasetConfig copy = new DatasetConfig(config.getDatasetName(), config.getDistribution(), config.getQueryRange());
        copy.setQueryBaseDir(config.getQueryBaseDir());
        copy.setDataBaseDir(config.getDataBaseDir());
        copy.setQueryType(config.getQueryType());
        copy.setNodes(config.getNodes());
        copy.setResolution(config.getResolution());
        copy.setAlpha(config.getAlpha());
        copy.setBeta(config.getBeta());
        copy.setMinTraj(config.getMinTraj());
        copy.setShards(config.getShards());
        copy.setThetaConfig(config.getThetaConfig());
        copy.setBmtreeConfigPath(config.getBmtreeConfigPath());
        copy.setBmtreeBitLength(config.getBmtreeBitLength());
        return copy;
    }

    private String buildTableName(MethodVariant variant, DatasetConfig dataset, String study, String sweepValue) {
        String normalizedSweep = sweepValue.toLowerCase(Locale.ROOT)
                .replace("*", "x")
                .replace("(", "")
                .replace(")", "")
                .replace(" ", "_")
                .replace("-", "_");
        return String.format(Locale.ROOT, "%s_%s_%s_%s",
                dataset.getDatasetName().toLowerCase(Locale.ROOT).replace("-", "_"),
                variant.tableKey,
                study,
                normalizedSweep);
    }

    private List<MethodVariant> standardVariants() {
        return Arrays.asList(
                MethodVariant.leti("LETI", "leti", true, null),
                MethodVariant.standard("TShape", "tshape", IndexMethod.TSHAPE),
                MethodVariant.standard("XZ*", "xzstar", IndexMethod.XZ_STAR),
                MethodVariant.lmsfc(),
                MethodVariant.bmtree()
        );
    }

    private List<MethodVariant> alphaBetaVariants() {
        return Arrays.asList(
                MethodVariant.leti("LETI", "leti", true, null),
                MethodVariant.standard("TShape", "tshape", IndexMethod.TSHAPE),
                MethodVariant.standard("XZ*", "xzstar", IndexMethod.XZ_STAR)
        );
    }

    private List<MethodVariant> indexSizeVariants() {
        return alphaBetaVariants();
    }

    private interface Configurator {
        void configure(DatasetConfig dataset, TableConfig config, String study);
    }

    private static class MethodVariant {
        final String label;
        final String tableKey;
        final IndexMethod baseMethod;
        final Configurator configurator;

        MethodVariant(String label, String tableKey, IndexMethod baseMethod, Configurator configurator) {
            this.label = label;
            this.tableKey = tableKey;
            this.baseMethod = baseMethod;
            this.configurator = configurator;
        }

        void configure(DatasetConfig dataset, TableConfig config, String study) {
            if (configurator != null) {
                configurator.configure(dataset, config, study);
            }
        }

        String resolvePendingReason(DatasetConfig dataset, String study) {
            if (baseMethod != IndexMethod.LETI) {
                return null;
            }
            return resolveOrderingSelection(dataset, null, study).pendingReason;
        }

        static MethodVariant standard(String label, String tableKey, IndexMethod method) {
            return new MethodVariant(label, tableKey, method, new Configurator() {
                @Override
                public void configure(DatasetConfig dataset, TableConfig config, String study) {
                    if (method == IndexMethod.LMSFC) {
                        config.setIsLMSFC(1);
                        config.setThetaConfig(dataset.getThetaConfigOrDefault());
                    } else if (method == IndexMethod.BMTREE) {
                        config.setIsBMTree(1);
                        config.setBMTreeConfigPath(dataset.getBMTreeConfigPathOrDefault());
                        config.setBMTreeBitLength(dataset.getBMTreeBitLengthOrDefault());
                    }
                }
            });
        }

        static MethodVariant leti(final String label, String tableKey, final boolean adaptivePartition, final String orderingPath) {
            return new MethodVariant(label, tableKey, IndexMethod.LETI, new Configurator() {
                @Override
                public void configure(DatasetConfig dataset, TableConfig config, String study) {
                    config.setAdaptivePartition(adaptivePartition ? 1 : 0);
                    OrderSelection selection = resolveOrderingSelection(dataset, orderingPath, study);
                    if (selection.pendingReason != null) {
                        throw new IllegalStateException(selection.pendingReason);
                    }
                    config.setOrderDefinitionPath(selection.path);
                }
            });
        }

        static MethodVariant lmsfc() {
            return standard("LMSFC", "lmsfc", IndexMethod.LMSFC);
        }

        static MethodVariant bmtree() {
            return standard("BMT", "bmt", IndexMethod.BMTREE);
        }

        private static String normalizeDistribution(String distribution) {
            if (distribution == null) {
                return "skew";
            }
            switch (distribution.toLowerCase(Locale.ROOT)) {
                case "uniform":
                    return "uni";
                case "gaussian":
                    return "gauss";
                case "skewed":
                    return "skew";
                default:
                    return distribution.toLowerCase(Locale.ROOT);
            }
        }

        private static OrderSelection resolveOrderingSelection(DatasetConfig dataset, String explicitOrderingPath, String study) {
            if (explicitOrderingPath != null && !explicitOrderingPath.trim().isEmpty()) {
                return OrderSelection.value(LetiOrderResolver.resolveClasspathResource(explicitOrderingPath));
            }

            List<String> specializedCandidates = specializedCandidates(dataset, study);
            if (!specializedCandidates.isEmpty()) {
                String resolved = firstExistingResource(specializedCandidates);
                if (resolved != null) {
                    return OrderSelection.value(resolved);
                }
                String reason = String.format(Locale.ROOT,
                        "PENDING(%s LETI RLOrder missing; expected one of: %s)",
                        study,
                        String.join(" | ", specializedCandidates));
                System.out.println("[ParameterExperiment] " + reason);
                return OrderSelection.pending(reason);
            }

            String candidate = String.format(Locale.ROOT, "leti/%s/%s_order.json",
                    dataset.getDatasetName().toLowerCase(Locale.ROOT).replace('-', '_'),
                    normalizeDistribution(dataset.getDistribution()));
            ClassLoader classLoader = ParameterExperiment.class.getClassLoader();
            if (classLoader.getResource(candidate) != null) {
                return OrderSelection.value(LetiOrderResolver.resolveClasspathResource(candidate));
            }
            return OrderSelection.value(LetiOrderResolver.defaultOrderPath());
        }

        private static List<String> specializedCandidates(DatasetConfig dataset, String study) {
            String datasetKey = dataset.getDatasetName().toLowerCase(Locale.ROOT).replace('-', '_');
            List<String> candidates = new ArrayList<>();
            if ("resolution".equals(study)) {
                int resolution = dataset.getResolution();
                candidates.add(String.format(Locale.ROOT, "leti/%s/skew_resolution_%d_order.json", datasetKey, resolution));
                candidates.add(String.format(Locale.ROOT, "leti/%s/resolution_%d_skew_order.json", datasetKey, resolution));
                candidates.add(String.format(Locale.ROOT, "leti/%s/resolution/%d_order.json", datasetKey, resolution));
            } else if ("minTraj".equals(study)) {
                int minTraj = dataset.getMinTraj();
                candidates.add(String.format(Locale.ROOT, "leti/%s/skew_mintraj_%d_order.json", datasetKey, minTraj));
                candidates.add(String.format(Locale.ROOT, "leti/%s/mintraj_%d_skew_order.json", datasetKey, minTraj));
                candidates.add(String.format(Locale.ROOT, "leti/%s/mintraj/%d_order.json", datasetKey, minTraj));
            }
            return candidates;
        }

        private static String firstExistingResource(List<String> candidates) {
            ClassLoader classLoader = ParameterExperiment.class.getClassLoader();
            for (String candidate : candidates) {
                if (classLoader.getResource(candidate) != null) {
                    return LetiOrderResolver.resolveClasspathResource(candidate);
                }
            }
            return null;
        }
    }

    private static final class OrderSelection {
        final String path;
        final String pendingReason;

        private OrderSelection(String path, String pendingReason) {
            this.path = path;
            this.pendingReason = pendingReason;
        }

        static OrderSelection value(String path) {
            return new OrderSelection(path, null);
        }

        static OrderSelection pending(String reason) {
            return new OrderSelection(null, reason);
        }
    }

    private static final class RunOutcome {
        final ExperimentStats stats;
        final String pendingReason;

        private RunOutcome(ExperimentStats stats, String pendingReason) {
            this.stats = stats;
            this.pendingReason = pendingReason;
        }

        static RunOutcome value(ExperimentStats stats) {
            return new RunOutcome(stats, null);
        }

        static RunOutcome pending(String pendingReason) {
            return new RunOutcome(null, pendingReason);
        }

        boolean isPending() {
            return pendingReason != null && !pendingReason.isEmpty();
        }
    }

    private static class ResultRow {
        final String study;
        final String dataset;
        final String metric;
        final String method;
        final String sweepValue;
        final String value;
        final String status;

        ResultRow(String study, String dataset, String metric, String method, String sweepValue, String value, String status) {
            this.study = study;
            this.dataset = dataset;
            this.metric = metric;
            this.method = method;
            this.sweepValue = sweepValue;
            this.value = value;
            this.status = status;
        }

        static ResultRow value(String study, String dataset, String metric, String method, String sweepValue, String value) {
            return new ResultRow(study, dataset, metric, method, sweepValue, value, "OK");
        }

        static ResultRow pending(String study, String dataset, String metric, String method, String sweepValue, String reason) {
            return new ResultRow(study, dataset, metric, method, sweepValue, "", reason);
        }

        String renderedValue() {
            return value == null || value.isEmpty() ? status : value;
        }
    }

    public static void main(String[] args) {
        if (args.length < 4) {
            System.out.println("Usage: ParameterExperiment <query_base_dir> <data_base_dir> <output_dir> <dataset_name>");
            return;
        }
        ParameterExperiment experiment = new ParameterExperiment(args[0], args[1], args[2], args[3]);
        experiment.runDefaultStudy();
    }
}
