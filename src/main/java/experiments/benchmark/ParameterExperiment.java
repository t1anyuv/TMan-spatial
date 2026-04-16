package experiments.benchmark;

import config.TableConfig;
import experiments.benchmark.config.DatasetConfig;
import experiments.benchmark.config.IndexMethod;
import experiments.benchmark.io.BenchmarkTableCleaner;
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
import java.nio.file.Path;
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
    private static final List<Integer> MIN_TRAJ_VALUES = Arrays.asList(4, 6, 8, 10);
    private static final List<String> METRICS = Arrays.asList(
            "Latency_ms", "LogicalIndexRanges", "VisitedCells", "RowKeyRanges");

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
            initializeSummaryCsv();
            runByResolution(rows);
            writeFlatResults(rows);
            writeResolutionMinTrajTables(rows);
        } catch (IOException e) {
            throw new RuntimeException("Failed to run parameter experiment", e);
        } finally {
            cleanupTables();
        }
    }

    private void runByResolution(List<ResultRow> rows) throws IOException {
        for (int resolution : RESOLUTION_VALUES) {
            System.out.println("[ParameterExperiment] Starting resolution " + resolution + " with minTraj values " + MIN_TRAJ_VALUES);
            List<ResultRow> resolutionRows = new ArrayList<>();
            try {
                for (int minTraj : MIN_TRAJ_VALUES) {
                    DatasetConfig config = defaultConfig();
                    config.setResolution(resolution);
                    config.setMinTraj(minTraj);
                    ExperimentStats stats = runOne(config, "resolution_minTraj", resolution + "_" + minTraj);
                    addRows(resolutionRows, "resolution_minTraj", config.getDatasetName(), "LETI",
                            buildSweepValue(resolution, minTraj), stats);
                }
            } catch (Throwable t) {
                addFailureRows(resolutionRows, resolution, t);
                System.err.println("[ParameterExperiment] Resolution " + resolution + " failed: " + t.getMessage());
            }
            rows.addAll(resolutionRows);
            writeResolutionFile(resolution, resolutionRows);
            appendSummaryRows(resolutionRows);
            printResolutionResults(resolution, resolutionRows);
        }
    }

    private ExperimentStats runOne(DatasetConfig dataset, String study, String sweepValue) {
        String orderPath = resolveOrderingSelection(dataset, study);
        String tableName = buildTableName(dataset, study, sweepValue);
        ensureTableExists(dataset, tableName, orderPath);
        return runner.runBenchmark(IndexMethod.LETI, dataset, tableName);
    }

    private void ensureTableExists(DatasetConfig dataset, String tableName, String orderPath) {
        if (ensuredTables.contains(tableName)) {
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

                TableConfig config = TableBuilder.buildConfig(dataset);
                config.setAdaptivePartition(1);
                config.setOrderDefinitionPath(orderPath);

                TableBuilder builder = new TableBuilder(dataset.getDataFilePath(), outputDir);
                builder.createTable(IndexMethod.LETI, tableName, config);
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
                    BenchmarkTableCleaner.deleteTableArtifacts(admin, tableName, "127.0.0.1");
                }
            }
        } catch (Exception e) {
            System.err.println("Error cleaning parameter experiment tables: " + e.getMessage());
        }
    }

    private void addRows(List<ResultRow> rows, String study, String dataset, String method, String sweepValue,
                         ExperimentStats stats) {
        for (String metric : METRICS) {
            rows.add(ResultRow.value(study, dataset, metric, method, sweepValue, formatMetric(metric, stats)));
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
            default:
                return "";
        }
    }

    private void addFailureRows(List<ResultRow> rows, int resolution, Throwable throwable) {
        String reason = throwable == null ? "FAILED" : sanitizeStatus(throwable.getClass().getSimpleName() + ":" + throwable.getMessage());
        for (int minTraj : MIN_TRAJ_VALUES) {
            String sweepValue = buildSweepValue(resolution, minTraj);
            for (String metric : METRICS) {
                rows.add(ResultRow.failure("resolution_minTraj", datasetName, metric, "LETI", sweepValue, reason));
            }
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
        for (int minTraj : MIN_TRAJ_VALUES) {
            columns.add("r6_m" + minTraj);
            columns.add("r7_m" + minTraj);
            columns.add("r8_m" + minTraj);
            columns.add("r9_m" + minTraj);
            columns.add("r10_m" + minTraj);
        }
        for (String metric : METRICS) {
            Map<String, Map<String, String>> grid = new LinkedHashMap<>();
            grid.put("LETI", new LinkedHashMap<String, String>());
            for (ResultRow row : rows) {
                if (!metric.equals(row.metric)) {
                    continue;
                }
                if (!"resolution_minTraj".equals(row.study)) {
                    continue;
                }
                Map<String, String> methodRow = grid.get(row.method);
                if (methodRow != null) {
                    methodRow.put(row.sweepValue, row.renderedValue());
                }
            }
            writePivot(Paths.get(outputDir, "parameter_resolution_minTraj_" + metric + ".csv").toString(), columns, grid);
        }
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
        DatasetConfig config = new DatasetConfig(datasetName, DatasetConfig.SKEWED, 1000);
        config.setQueryBaseDir(queryBaseDir);
        config.setDataBaseDir(dataBaseDir);
        config.setQueryType("SRQ");
        config.setNodes(4);
        config.setResolution(8);
        config.setAlpha(3);
        config.setBeta(3);
        config.setMinTraj(4);
        return config;
    }

    private void initializeSummaryCsv() throws IOException {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(summaryFilePath().toFile()))) {
            writer.write("Study,Dataset,Metric,Method,SweepValue,Value,Status");
            writer.newLine();
        }
    }

    private void appendSummaryRows(List<ResultRow> rows) throws IOException {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(summaryFilePath().toFile(), true))) {
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

    private Path summaryFilePath() {
        return Paths.get(outputDir, "parameter_results_progressive.csv");
    }

    private void writeResolutionFile(int resolution, List<ResultRow> rows) throws IOException {
        Path file = Paths.get(outputDir, String.format(Locale.ROOT, "parameter_resolution_%d.csv", resolution));
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(file.toFile()))) {
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

    private String buildSweepValue(int resolution, int minTraj) {
        return String.format(Locale.ROOT, "r%d_m%d", resolution, minTraj);
    }

    private String buildTableName(DatasetConfig dataset, String study, String sweepValue) {
        String normalizedSweep = sweepValue.toLowerCase(Locale.ROOT).replace(" ", "_").replace("-", "_");
        return String.format(Locale.ROOT, "%s_leti_%s_%s",
                dataset.getDatasetName().toLowerCase(Locale.ROOT).replace("-", "_"),
                study,
                normalizedSweep);
    }

    private String resolveOrderingSelection(DatasetConfig dataset, String study) {
        String datasetKey = dataset.getDatasetName().toLowerCase(Locale.ROOT).replace('-', '_');
        String candidate;
        if ("resolution_minTraj".equals(study)) {
            candidate = String.format(Locale.ROOT,
                    "leti/%s/param/quadorder_skewed_res%d_min%d.json",
                    datasetKey,
                    dataset.getResolution(),
                    dataset.getMinTraj());
        } else {
            candidate = String.format(Locale.ROOT, "leti/%s/param/skew_order.json", datasetKey);
        }
        String resolved = LetiOrderResolver.resolveClasspathResource(candidate);
        if (ParameterExperiment.class.getClassLoader().getResource(resolved) == null) {
            throw new IllegalStateException("LETI order resource not found: " + candidate);
        }
        return resolved;
    }

    private void printResolutionResults(int resolution, List<ResultRow> rows) {
        System.out.println("[ParameterExperiment] Completed resolution " + resolution);
        for (ResultRow row : rows) {
            System.out.printf(Locale.ROOT,
                    "  [resolution=%d][%s] %s -> %s%n",
                    resolution,
                    row.metric,
                    row.sweepValue,
                    row.renderedValue());
        }
        System.out.println();
    }

    private String sanitizeStatus(String value) {
        if (value == null || value.isEmpty()) {
            return "FAILED";
        }
        return value.replace("\r", " ").replace("\n", " ").replace(",", ";");
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

        static ResultRow failure(String study, String dataset, String metric, String method, String sweepValue, String status) {
            return new ResultRow(study, dataset, metric, method, sweepValue, "", status);
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
