package experiments.suite.similarity;

import config.TableConfig;
import entity.Trajectory;
import experiments.common.ExperimentDefaults;
import experiments.common.config.DatasetConfig;
import experiments.common.config.IndexMethod;
import experiments.common.io.BenchmarkTableCleaner;
import experiments.common.io.TableBuilder;
import experiments.standalone.query.TrajectorySimilarityQuerySupport;
import experiments.suite.similarity.config.SimilarityExperimentConfig;
import experiments.suite.similarity.config.SimilarityExperimentDataset;
import experiments.suite.similarity.config.SimilarityQueryType;
import experiments.suite.similarity.model.SimilarityExperimentResult;
import experiments.suite.similarity.output.SimilaritySummaryWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import similarity.TrajectorySimilarity;
import utils.LetiOrderResolver;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;

public class SimilarityExperimentRunner {
    private final SimilarityExperimentConfig config;
    private final Set<String> createdTables = new LinkedHashSet<>();

    public SimilarityExperimentRunner(SimilarityExperimentConfig config) {
        this.config = config;
    }

    public void run() throws IOException {
        List<SimilarityExperimentResult> results = new ArrayList<>();
        try {
            for (SimilarityExperimentDataset dataset : config.getDatasets()) {
                List<Trajectory> queries = loadQueryTrajectories(dataset.getQueryWorkloadPath(), config.getQueryLimit());
                for (IndexMethod method : config.getMethods()) {
                    String tableName = buildTableName(dataset, method);
                    DatasetConfig datasetConfig = buildDatasetConfig(dataset);
                    TableConfig tableConfig = buildTableConfig(datasetConfig, method);
                    ensureTable(tableName, datasetConfig, tableConfig, method);

                    try (TrajectorySimilarityQuerySupport support = new TrajectorySimilarityQuerySupport()) {
                        for (int distanceFunction : config.getDistanceFunctions()) {
                            for (SimilarityQueryType queryType : config.getQueryTypes()) {
                                switch (queryType) {
                                    case TOP_K:
                                        for (Integer k : config.getTopKValues()) {
                                            results.add(runTopKCase(dataset, method, distanceFunction, k, tableName, queries, support));
                                        }
                                        break;
                                    case SIMILARITY:
                                        for (Double threshold : config.getSimilarityThresholds()) {
                                            results.add(runSimilarityCase(dataset, method, distanceFunction, threshold, tableName, queries, support));
                                        }
                                        break;
                                    default:
                                        break;
                                }
                            }
                        }
                    }
                }
            }
        } finally {
            if (config.isCleanupTablesAfterRun()) {
                cleanupTables();
            }
        }

        Path outputFile = Paths.get(config.getOutputDir(), "similarity_query_summary.csv");
        SimilaritySummaryWriter.write(outputFile, results);
        System.out.println("Similarity-query summary written to: " + outputFile);
    }

    private SimilarityExperimentResult runTopKCase(SimilarityExperimentDataset dataset,
                                                   IndexMethod method,
                                                   int distanceFunction,
                                                   int k,
                                                   String tableName,
                                                   List<Trajectory> queries,
                                                   TrajectorySimilarityQuerySupport support) throws IOException {
        System.out.printf(Locale.ROOT, "%n[SimilarityExperiment] dataset=%s type=top_k k=%d distance=%s method=%s%n",
                dataset.getName(), k, TrajectorySimilarity.functionName(distanceFunction), method.getShortName());

        long latencySum = 0L;
        long rowKeyRangeSum = 0L;
        long candidateSum = 0L;
        long finalSizeSum = 0L;
        long iterationSum = 0L;

        for (Trajectory query : queries) {
            long startNs = System.nanoTime();
            TrajectorySimilarityQuerySupport.TopKQueryResult result =
                    support.topKQueryWithStats(tableName, query, k, distanceFunction, config.getTopKInitialThreshold());
            long latencyMs = (System.nanoTime() - startNs) / 1_000_000L;

            latencySum += latencyMs;
            rowKeyRangeSum += result.getTotalRowKeyRangeCount();
            candidateSum += result.getTotalCandidateCount();
            finalSizeSum += result.getResults().size();
            iterationSum += result.getIterations();
        }

        return new SimilarityExperimentResult(
                dataset.getName(),
                SimilarityQueryType.TOP_K.getConfigValue(),
                "k",
                String.valueOf(k),
                TrajectorySimilarity.functionName(distanceFunction),
                method,
                avg(latencySum, queries.size()),
                avg(rowKeyRangeSum, queries.size()),
                avg(candidateSum, queries.size()),
                avg(finalSizeSum, queries.size()),
                avg(iterationSum, queries.size()),
                queries.size());
    }

    private SimilarityExperimentResult runSimilarityCase(SimilarityExperimentDataset dataset,
                                                         IndexMethod method,
                                                         int distanceFunction,
                                                         double threshold,
                                                         String tableName,
                                                         List<Trajectory> queries,
                                                         TrajectorySimilarityQuerySupport support) throws IOException {
        System.out.printf(Locale.ROOT, "%n[SimilarityExperiment] dataset=%s type=similarity threshold=%.3f distance=%s method=%s%n",
                dataset.getName(), threshold, TrajectorySimilarity.functionName(distanceFunction), method.getShortName());

        long latencySum = 0L;
        long rowKeyRangeSum = 0L;
        long candidateSum = 0L;
        long finalSizeSum = 0L;

        for (Trajectory query : queries) {
            long startNs = System.nanoTime();
            TrajectorySimilarityQuerySupport.SimilarityQueryResult result =
                    support.similarityQueryWithStats(tableName, query, threshold, distanceFunction);
            long latencyMs = (System.nanoTime() - startNs) / 1_000_000L;

            latencySum += latencyMs;
            rowKeyRangeSum += result.getRowKeyRangeCount();
            candidateSum += result.getCandidateCount();
            finalSizeSum += result.getResults().size();
        }

        return new SimilarityExperimentResult(
                dataset.getName(),
                SimilarityQueryType.SIMILARITY.getConfigValue(),
                "threshold",
                String.format(Locale.US, "%.3f", threshold),
                TrajectorySimilarity.functionName(distanceFunction),
                method,
                avg(latencySum, queries.size()),
                avg(rowKeyRangeSum, queries.size()),
                avg(candidateSum, queries.size()),
                avg(finalSizeSum, queries.size()),
                1.0d,
                queries.size());
    }

    private List<Trajectory> loadQueryTrajectories(String path, int queryLimit) throws IOException {
        List<String> lines = Files.readAllLines(Paths.get(path)).stream()
                .map(String::trim)
                .filter(line -> !line.isEmpty())
                .collect(Collectors.toList());
        if (lines.isEmpty()) {
            throw new IllegalStateException("No query trajectories found in " + path);
        }

        int effectiveLimit = queryLimit > 0 ? Math.min(queryLimit, lines.size()) : lines.size();
        List<Trajectory> queries = new ArrayList<>(effectiveLimit);
        for (int i = 0; i < effectiveLimit; i++) {
            queries.add(TrajectorySimilarityQuerySupport.parseQueryTrajectory(lines.get(i)));
        }
        return queries;
    }

    private DatasetConfig buildDatasetConfig(SimilarityExperimentDataset dataset) {
        DatasetConfig datasetConfig = new DatasetConfig(dataset.getName());
        datasetConfig.setDataFilePath(dataset.getDataFilePath());
        datasetConfig.setResolution(ExperimentDefaults.DEFAULT_RESOLUTION);
        datasetConfig.setMinTraj(ExperimentDefaults.DEFAULT_MIN_TRAJ);
        datasetConfig.setAlpha(ExperimentDefaults.DEFAULT_ALPHA);
        datasetConfig.setBeta(ExperimentDefaults.DEFAULT_BETA);
        datasetConfig.setLetiAlpha(ExperimentDefaults.DEFAULT_ALPHA);
        datasetConfig.setLetiBeta(ExperimentDefaults.DEFAULT_BETA);
        datasetConfig.setNodes(ExperimentDefaults.DEFAULT_NODES);
        return datasetConfig;
    }

    private TableConfig buildTableConfig(DatasetConfig datasetConfig, IndexMethod method) {
        TableConfig tableConfig = TableBuilder.buildConfig(datasetConfig);
        switch (method) {
            case LETI:
                tableConfig.setAdaptivePartition(1);
                tableConfig.setIsXZ(0);
                tableConfig.setOrderEncodingType(1);
                tableConfig.setTspEncoding(1);
                tableConfig.setAlpha(datasetConfig.getLetiAlphaOrDefault());
                tableConfig.setBeta(datasetConfig.getLetiBetaOrDefault());
                tableConfig.setOrderDefinitionPath(resolveSimilarityLetiOrder(datasetConfig));
                break;
            case TSHAPE:
                tableConfig.setIsXZ(0);
                tableConfig.setTspEncoding(1);
                break;
            case XZ_STAR:
                tableConfig.setIsXZ(1);
                tableConfig.setTspEncoding(0);
                tableConfig.setAlpha(2);
                tableConfig.setBeta(2);
                break;
            default:
                throw new IllegalArgumentException("Unsupported similarity experiment method: " + method);
        }
        return tableConfig;
    }

    private String resolveSimilarityLetiOrder(DatasetConfig datasetConfig) {
        String resource = String.format(Locale.ROOT, "leti/%s/param/skewed_r%d_min%d_a3_b3.json",
                datasetResourceKey(datasetConfig.getDatasetName()),
                datasetConfig.getResolution(),
                datasetConfig.getMinTraj());
        if (SimilarityExperimentRunner.class.getClassLoader().getResource(resource) == null) {
            throw new IllegalStateException("Missing LETI similarity-query order file: " + resource);
        }
        return LetiOrderResolver.resolveClasspathResource(resource);
    }

    private void ensureTable(String tableName, DatasetConfig datasetConfig, TableConfig tableConfig, IndexMethod method) {
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
                TableBuilder builder = new TableBuilder(datasetConfig.getDataFilePath(), config.getOutputDir());
                builder.createTable(method, tableName, tableConfig);
                createdTables.add(tableName);
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to prepare similarity-query table " + tableName, e);
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
            System.err.println("Error cleaning similarity-query experiment tables: " + e.getMessage());
        }
    }

    private String buildTableName(SimilarityExperimentDataset dataset, IndexMethod method) {
        return String.format(Locale.ROOT, "%s_%s_simq",
                datasetResourceKey(dataset.getName()),
                method.getShortName().toLowerCase(Locale.ROOT));
    }

    private String datasetResourceKey(String datasetName) {
        String normalized = datasetName == null ? "" : datasetName.trim().toLowerCase(Locale.ROOT).replace('-', '_');
        if ("cdtaxi".equals(normalized) || "cd_taxi".equals(normalized)) {
            return "cd_taxi";
        }
        return "tdrive";
    }

    private double avg(long total, int count) {
        return count <= 0 ? 0.0d : ((double) total) / count;
    }
}
