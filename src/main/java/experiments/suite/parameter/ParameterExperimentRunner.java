package experiments.suite.parameter;

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
import experiments.suite.parameter.model.ParameterBuildRow;
import experiments.suite.parameter.model.ParameterExperimentRow;
import experiments.suite.parameter.output.ParameterBuildSummaryWriter;
import experiments.suite.parameter.output.ParameterSummaryWriter;
import loader.Loader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import utils.LSFCReader;
import utils.LetiOrderResolver;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

public class ParameterExperimentRunner {
    private final String queryBaseDir;
    private final String dataFilePath;
    private final String outputDir;
    private final String datasetName;
    private final PlannerBackedQueryExecutor queryExecutor = new PlannerBackedQueryExecutor();
    private final Set<String> createdTables = new LinkedHashSet<>();

    public ParameterExperimentRunner(String queryBaseDir, String dataFilePath, String outputDir) {
        this(queryBaseDir, dataFilePath, outputDir, null);
    }

    public ParameterExperimentRunner(String queryBaseDir, String dataFilePath, String outputDir, String datasetName) {
        this.queryBaseDir = queryBaseDir;
        this.dataFilePath = dataFilePath;
        this.outputDir = outputDir;
        this.datasetName = datasetName;
    }

    public void run() throws IOException {
        List<ParameterExperimentRow> rows = new ArrayList<>();
        List<ParameterBuildRow> buildRows = new ArrayList<>();
        try {
            for (int resolution : ExperimentDefaults.PARAMETER_RESOLUTION_VALUES) {
                rows.add(runCase("resolution", resolution, ExperimentDefaults.DEFAULT_MIN_TRAJ, buildRows));
            }
            for (int minTraj : ExperimentDefaults.PARAMETER_MIN_TRAJ_VALUES) {
                rows.add(runCase("min_traj", ExperimentDefaults.DEFAULT_RESOLUTION, minTraj, buildRows));
            }
        } finally {
            cleanupTables();
        }

        Path outputFile = Paths.get(outputDir, "parameter_summary.csv");
        ParameterSummaryWriter.write(outputFile, rows);
        System.out.println("Parameter summary written to: " + outputFile);

        Path buildOutputFile = Paths.get(outputDir, "parameter_build_summary.csv");
        ParameterBuildSummaryWriter.write(buildOutputFile, buildRows);
        System.out.println("Parameter build summary written to: " + buildOutputFile);
    }

    private ParameterExperimentRow runCase(String study,
                                           int resolution,
                                           int minTraj,
                                           List<ParameterBuildRow> buildRows) throws IOException {
        DatasetConfig datasetConfig = baseDatasetConfig();
        datasetConfig.setResolution(resolution);
        datasetConfig.setMinTraj(minTraj);
        String orderPath = resolveOrderPath(datasetConfig, resolution, minTraj);
        long rc = loadRc(orderPath);

        String tableName = buildTableName(datasetConfig, resolution, minTraj);
        TableConfig tableConfig = buildTableConfig(datasetConfig, orderPath);
        Loader.StoreSummary storeSummary = ensureTable(tableName, datasetConfig, tableConfig);
        buildRows.add(toBuildRow(study, datasetConfig, tableName, resolution, minTraj, storeSummary));

        BasicQuery queryStrategy = PlannerBackedQueryExecutor.createQueryStrategy(tableConfig);
        List<QueryLoader.QueryWindow> queries = new QueryLoader().loadRangeQueries(datasetConfig);
        if (queries.isEmpty()) {
            throw new IllegalStateException("No queries found for parameter experiment");
        }

        long latencySum = 0L;
        long rkiSum = 0L;
        long ctSum = 0L;
        int countedQueries = 0;
        try (PlannerBackedQueryExecutor.QuerySession session = queryExecutor.openSession(tableName, tableConfig, queryStrategy)) {
            session.warmup(queries.get(0).toCsvString());

            for (int i = 1; i < queries.size(); i++) {
                QueryMetrics metrics = session.execute(queries.get(i).toCsvString());
                latencySum += metrics.getLatencyMs();
                rkiSum += metrics.getRki();
                ctSum += metrics.getCt();
                countedQueries++;
            }

            if (countedQueries == 0) {
                QueryMetrics metrics = session.execute(queries.get(0).toCsvString());
                latencySum += metrics.getLatencyMs();
                rkiSum += metrics.getRki();
                ctSum += metrics.getCt();
                countedQueries = 1;
            }
        }

        return new ParameterExperimentRow(
                detectDatasetName(),
                study,
                resolution,
                minTraj,
                rc,
                ((double) latencySum) / countedQueries,
                ((double) rkiSum) / countedQueries,
                ((double) ctSum) / countedQueries,
                countedQueries);
    }

    private DatasetConfig baseDatasetConfig() {
        DatasetConfig datasetConfig = new DatasetConfig(
                detectDatasetName(),
                ExperimentDefaults.DEFAULT_DISTRIBUTION,
                ExperimentDefaults.DEFAULT_QUERY_RANGE);
        datasetConfig.setQueryBaseDir(queryBaseDir);
        datasetConfig.setDataFilePath(dataFilePath);
        datasetConfig.setQueryType(ExperimentDefaults.DEFAULT_QUERY_TYPE);
        datasetConfig.setNodes(ExperimentDefaults.DEFAULT_NODES);
        datasetConfig.setAlpha(ExperimentDefaults.DEFAULT_ALPHA);
        datasetConfig.setBeta(ExperimentDefaults.DEFAULT_BETA);
        datasetConfig.setLetiAlpha(ExperimentDefaults.DEFAULT_ALPHA);
        datasetConfig.setLetiBeta(ExperimentDefaults.DEFAULT_BETA);
        return datasetConfig;
    }

    private TableConfig buildTableConfig(DatasetConfig datasetConfig, String orderPath) {
        TableConfig tableConfig = TableBuilder.buildConfig(datasetConfig);
        tableConfig.setAdaptivePartition(1);
        tableConfig.setIsXZ(0);
        tableConfig.setOrderEncodingType(1);
        tableConfig.setTspEncoding(1);
        tableConfig.setAlpha(datasetConfig.getLetiAlphaOrDefault());
        tableConfig.setBeta(datasetConfig.getLetiBetaOrDefault());
        tableConfig.setOrderDefinitionPath(orderPath);
        return tableConfig;
    }

    private Loader.StoreSummary ensureTable(String tableName, DatasetConfig datasetConfig, TableConfig tableConfig) {
        if (createdTables.contains(tableName)) {
            return null;
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
                Loader.StoreSummary summary = builder.createTable(IndexMethod.LETI, tableName, tableConfig);
                createdTables.add(tableName);
                return summary;
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to prepare parameter experiment table " + tableName, e);
        }
    }

    private ParameterBuildRow toBuildRow(String study,
                                         DatasetConfig datasetConfig,
                                         String tableName,
                                         int resolution,
                                         int minTraj,
                                         Loader.StoreSummary summary) {
        long trajectoryCount = 0L;
        long nodeCount = 0L;
        long shapeCount = 0L;
        long buildTimeMs = 0L;
        long mainTableBytes = 0L;
        long indexTableBytes = 0L;
        long extraIndexInfoBytes = 0L;
        long totalBytes = 0L;
        String note = "";

        if (summary != null) {
            trajectoryCount = summary.getTrajectoryCount();
            nodeCount = summary.getNodeCount();
            shapeCount = summary.getShapeCount();
            buildTimeMs = summary.getIndexingTimeMs();
            mainTableBytes = summary.getMainTableBytes();
            indexTableBytes = summary.getIndexTableBytes();
            extraIndexInfoBytes = summary.getExtraIndexInfoBytes();
            totalBytes = summary.getTotalBytes();
            note = summary.getNote();
        }

        return new ParameterBuildRow(
                datasetConfig.getDatasetName(),
                tableName,
                study,
                resolution,
                minTraj,
                trajectoryCount,
                nodeCount,
                shapeCount,
                buildTimeMs,
                mainTableBytes,
                indexTableBytes,
                extraIndexInfoBytes,
                totalBytes,
                note);
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

    private String resolveOrderPath(DatasetConfig datasetConfig, int resolution, int minTraj) {
        int letiAlpha = datasetConfig.getLetiAlphaOrDefault();
        int letiBeta = datasetConfig.getLetiBetaOrDefault();
        String resource = String.format(Locale.ROOT,
                "leti/%s/param/skewed_r%d_min%d_a%d_b%d.json",
                datasetResourceKey(),
                resolution,
                minTraj,
                letiAlpha,
                letiBeta);
        if (ParameterExperimentRunner.class.getClassLoader().getResource(resource) == null) {
            throw new IllegalStateException("Missing LETI parameter order file: " + resource);
        }
        return LetiOrderResolver.resolveClasspathResource(resource);
    }

    private long loadRc(String orderPath) throws IOException {
        LSFCReader.EffectiveNodeIndex index = LSFCReader.loadEffectiveOnlyFromClasspath(orderPath);
        return index.metadata == null ? 0L : index.metadata.activeCells;
    }

    private String buildTableName(DatasetConfig datasetConfig, int resolution, int minTraj) {
        return String.format(Locale.ROOT, "%s_leti_param_r%d_m%d_a%d_b%d",
                datasetTableKey(),
                resolution,
                minTraj,
                datasetConfig.getLetiAlphaOrDefault(),
                datasetConfig.getLetiBetaOrDefault());
    }

    private String detectDatasetName() {
        if (datasetName != null && !datasetName.trim().isEmpty()) {
            String normalized = datasetName.trim().toLowerCase(Locale.ROOT).replace("-", "").replace("_", "");
            if ("tdrive".equals(normalized)) {
                return DatasetConfig.T_DRIVE;
            }
            if ("cdtaxi".equals(normalized) || "chengdutaxi".equals(normalized)) {
                return DatasetConfig.CD_TAXI;
            }
            throw new IllegalArgumentException("Unsupported dataset name: " + datasetName + ". Supported: Tdrive, CD-Taxi");
        }
        String normalized = (dataFilePath + " " + queryBaseDir).toLowerCase(Locale.ROOT).replace('\\', '/');
        if (normalized.contains("cdtaxi") || normalized.contains("cd_taxi") || normalized.contains("chengdu")) {
            return DatasetConfig.CD_TAXI;
        }
        return DatasetConfig.T_DRIVE;
    }

    private String datasetResourceKey() {
        return DatasetConfig.CD_TAXI.equalsIgnoreCase(detectDatasetName()) ? "cd_taxi" : "tdrive";
    }

    private String datasetTableKey() {
        return DatasetConfig.CD_TAXI.equalsIgnoreCase(detectDatasetName()) ? "cd_taxi" : "tdrive";
    }
}
