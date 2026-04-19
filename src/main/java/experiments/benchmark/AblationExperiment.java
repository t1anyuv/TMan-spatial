package experiments.benchmark;

import config.TableConfig;
import experiments.benchmark.config.DatasetConfig;
import experiments.benchmark.config.IndexMethod;
import experiments.benchmark.io.BenchmarkTableCleaner;
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
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

public class AblationExperiment {
    private final String queryBaseDir;
    private final String dataBaseDir;
    private final String outputDir;
    private final String datasetName;
    private final BenchmarkRunner runner;
    private final Set<String> createdTables = new LinkedHashSet<>();
    @Setter
    private Integer letiAlpha;
    @Setter
    private Integer letiBeta;

    public AblationExperiment(String queryBaseDir, String dataBaseDir, String outputDir, String datasetName) {
        this.queryBaseDir = queryBaseDir;
        this.dataBaseDir = dataBaseDir;
        this.outputDir = outputDir;
        this.datasetName = datasetName;
        this.runner = new BenchmarkRunner(outputDir);
    }

    public void run() {
        try {
            Files.createDirectories(Paths.get(outputDir));
            DatasetConfig config = defaultConfig();
            List<Variant> variants = variants();
            Map<String, ExperimentStats> statsByVariant = new LinkedHashMap<>();

            for (Variant variant : variants) {
                String tableName = buildTableName(variant);
                ensureTable(variant, config, tableName);
                ExperimentStats stats = runner.runBenchmark(variant.method, config, tableName);
                preserveVariantBenchmarkCsv(config, variant);
                statsByVariant.put(variant.label, stats);
            }

            List<MetricsRow> metricsRows = new java.util.ArrayList<>();
            for (Variant variant : variants) {
                ExperimentStats stats = statsByVariant.get(variant.label);
                if (stats == null) {
                    throw new IllegalStateException("Missing stats for variant: " + variant.label);
                }
                metricsRows.add(MetricsRow.from(variant, stats));
            }

            writeMetricsCsv(metricsRows);
        } catch (IOException e) {
            throw new RuntimeException("Failed to run ablation experiment", e);
        } finally {
            cleanupTables();
        }
    }

    private DatasetConfig defaultConfig() {
        DatasetConfig config = new DatasetConfig(datasetName, DatasetConfig.SKEWED, 1000);
        config.setQueryBaseDir(queryBaseDir);
        config.setDataBaseDir(dataBaseDir);
        config.setResolution(8);
        config.setAlpha(3);
        config.setBeta(3);
        config.setLetiAlpha(3);
        config.setLetiBeta(3);
        config.setMinTraj(4);
        config.setNodes(4);
        config.setQueryType("SRQ");
        return config;
    }

    private List<Variant> variants() {
        return Arrays.asList(
                new Variant("LETI", true, OrderFamily.LETI),
                new Variant("LETI-BMT", true, OrderFamily.BMTREE),
                new Variant("LETI-LMSFC", true, OrderFamily.LMSFC),
                new Variant("LETI-ab", false, OrderFamily.LETI)
        );
    }

    private void ensureTable(Variant variant, DatasetConfig dataset, String tableName) {
        try {
            Configuration conf = HBaseConfiguration.create();
            try (Connection connection = ConnectionFactory.createConnection(conf);
                 Admin admin = connection.getAdmin()) {
                TableName hTableName = TableName.valueOf(tableName);
                if (admin.tableExists(hTableName)) {
                    BenchmarkTableCleaner.deleteTableArtifacts(admin, tableName, "127.0.0.1");
                }

                TableConfig config = TableBuilder.buildConfig(dataset);
                configureTable(dataset, config, variant);

                TableBuilder builder = new TableBuilder(dataset.getDataFilePath(), outputDir);
                builder.createTable(variant.method, tableName, config);
                createdTables.add(tableName);
            }
        } catch (Exception e) {
            throw new RuntimeException("Error creating table " + tableName, e);
        }
    }

    private void configureTable(DatasetConfig dataset, TableConfig config, Variant variant) {
        config.setAlpha(dataset.getLetiAlphaOrDefault());
        config.setBeta(dataset.getLetiBetaOrDefault());
        config.setAdaptivePartition(variant.adaptivePartition ? 1 : 0);
        config.setOrderDefinitionPath(resolveOrderingPath(dataset, variant));
    }

    private String resolveOrderingPath(DatasetConfig dataset, Variant variant) {
        if (variant.orderFamily == OrderFamily.BMTREE || variant.orderFamily == OrderFamily.LMSFC) {
            return resolveAlternativeOrderingPath(dataset, variant.orderFamily);
        }
        String datasetKey = dataset.getDatasetName().toLowerCase(Locale.ROOT).replace('-', '_');
        String candidate = String.format(Locale.ROOT, "leti/%s/param/quadorder_%s_res%d_min%d.json",
                datasetKey,
                normalizeFullDistribution(dataset.getDistribution()),
                dataset.getResolution(),
                dataset.getMinTraj());
        ClassLoader classLoader = AblationExperiment.class.getClassLoader();
        if (classLoader.getResource(candidate) != null) {
            return LetiOrderResolver.resolveClasspathResource(candidate);
        }
        String comparisonCandidate = String.format(Locale.ROOT, "leti/%s/comparison/%s_order.json",
                datasetKey,
                normalizeDistribution(dataset.getDistribution()));
        if (classLoader.getResource(comparisonCandidate) != null) {
            return LetiOrderResolver.resolveClasspathResource(comparisonCandidate);
        }
        return LetiOrderResolver.defaultOrderPath();
    }

    private String resolveAlternativeOrderingPath(DatasetConfig dataset, OrderFamily orderFamily) {
        String datasetKey = dataset.getDatasetName().toLowerCase(Locale.ROOT).replace('-', '_');
        String familyDir = orderFamily == OrderFamily.BMTREE ? "bmtree" : "lmsfc";
        String distCandidate = String.format(Locale.ROOT, "%s/%s/%s_order.json",
                familyDir,
                datasetKey,
                normalizeDistribution(dataset.getDistribution()));
        ClassLoader classLoader = AblationExperiment.class.getClassLoader();
        if (classLoader.getResource(distCandidate) != null) {
            return distCandidate;
        }
        String uniformFallback = String.format(Locale.ROOT, "%s/%s/uni_order.json", familyDir, datasetKey);
        if (classLoader.getResource(uniformFallback) != null) {
            return uniformFallback;
        }
        throw new IllegalStateException("Order resource not found for " + orderFamily + ": " + distCandidate);
    }

    private String normalizeDistribution(String distribution) {
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

    private String normalizeFullDistribution(String distribution) {
        if (distribution == null) {
            return "skewed";
        }
        return distribution.toLowerCase(Locale.ROOT);
    }

    private String buildTableName(Variant variant) {
        return String.format(Locale.ROOT, "%s_ablation_%s",
                datasetName.toLowerCase(Locale.ROOT).replace("-", "_"),
                variant.label.toLowerCase(Locale.ROOT)
                        .replace("(", "_")
                        .replace(")", "")
                        .replace("-", "_")
                        + (variant.method == IndexMethod.LETI
                        ? String.format(Locale.ROOT, "_a%d_b%d", resolveLetiAlpha(), resolveLetiBeta())
                        : ""));
    }

    private int resolveLetiAlpha() {
        return letiAlpha == null || letiAlpha <= 0 ? 3 : letiAlpha;
    }

    private int resolveLetiBeta() {
        return letiBeta == null || letiBeta <= 0 ? 3 : letiBeta;
    }

    private void writeMetricsCsv(List<MetricsRow> rows) throws IOException {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(Paths.get(outputDir, "ablation_metrics.csv").toFile()))) {
            writer.write("Variant,Dataset,Mode,Distribution,QueryRangeMeters,Resolution,Alpha,Beta,AdaptivePartition,OrderPath,"
                    + "LatencyAvg,LogicalIndexRangesAvg,QuadCodeRangesAvg,QOrderRangesAvg,RowKeyRangesAvg,"
                    + "CandidatesAvg,FinalResultCountAvg,VisitedCellsAvg,RedisAccessCountAvg,"
                    + "RedisShapeFilterRateScaledAvg,IndexSizeKB");
            writer.newLine();
            for (MetricsRow row : rows) {
                writer.write(row.toCsv());
                writer.newLine();
            }
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
            System.err.println("Error cleaning ablation tables: " + e.getMessage());
        }
    }

    private void preserveVariantBenchmarkCsv(DatasetConfig dataset, Variant variant) throws IOException {
        Path source = Paths.get(outputDir, String.format(Locale.ROOT, "%s_%s-%s-%dm.csv",
                variant.method.getShortName(),
                dataset.getDatasetName(),
                dataset.getDistribution(),
                dataset.getQueryRange()));
        if (!Files.exists(source)) {
            throw new IOException("Benchmark output not found: " + source);
        }
        Path target = Paths.get(outputDir, buildVariantBenchmarkFileName(dataset, variant));
        Files.copy(source, target, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
    }

    private String buildVariantBenchmarkFileName(DatasetConfig dataset, Variant variant) {
        return String.format(Locale.ROOT, "%s_%s-%s-%dm_%s.csv",
                variant.method.getShortName(),
                dataset.getDatasetName(),
                dataset.getDistribution(),
                dataset.getQueryRange(),
                sanitizeFileToken(variant.label));
    }

    private String sanitizeFileToken(String value) {
        return value == null ? "" : value.replaceAll("[^A-Za-z0-9._-]", "_");
    }

    private static class Variant {
        final String label;
        final IndexMethod method;
        final boolean adaptivePartition;
        final OrderFamily orderFamily;

        Variant(String label, boolean adaptivePartition, OrderFamily orderFamily) {
            this.label = label;
            this.method = IndexMethod.LETI;
            this.adaptivePartition = adaptivePartition;
            this.orderFamily = orderFamily;
        }
    }

    private enum OrderFamily {
        LETI,
        BMTREE,
        LMSFC
    }

    private static class MetricsRow {
        final String variantLabel;
        final String datasetName;
        final String mode;
        final String distribution;
        final int queryRange;
        final int resolution;
        final int alpha;
        final int beta;
        final int adaptivePartition;
        final String orderPath;
        final long latencyAvg;
        final long logicalIndexRangesAvg;
        final long quadCodeRangesAvg;
        final long qOrderRangesAvg;
        final long rowKeyRangesAvg;
        final long candidatesAvg;
        final long finalResultCountAvg;
        final long visitedCellsAvg;
        final long redisAccessCountAvg;
        final long redisShapeFilterRateScaledAvg;
        final long indexSizeKb;

        MetricsRow(String variantLabel,
                   String datasetName,
                   String mode,
                   String distribution,
                   int queryRange,
                   int resolution,
                   int alpha,
                   int beta,
                   int adaptivePartition,
                   String orderPath,
                   long latencyAvg,
                   long logicalIndexRangesAvg,
                   long quadCodeRangesAvg,
                   long qOrderRangesAvg,
                   long rowKeyRangesAvg,
                   long candidatesAvg,
                   long finalResultCountAvg,
                   long visitedCellsAvg,
                   long redisAccessCountAvg,
                   long redisShapeFilterRateScaledAvg,
                   long indexSizeKb) {
            this.variantLabel = variantLabel;
            this.datasetName = datasetName;
            this.mode = mode;
            this.distribution = distribution;
            this.queryRange = queryRange;
            this.resolution = resolution;
            this.alpha = alpha;
            this.beta = beta;
            this.adaptivePartition = adaptivePartition;
            this.orderPath = orderPath;
            this.latencyAvg = latencyAvg;
            this.logicalIndexRangesAvg = logicalIndexRangesAvg;
            this.quadCodeRangesAvg = quadCodeRangesAvg;
            this.qOrderRangesAvg = qOrderRangesAvg;
            this.rowKeyRangesAvg = rowKeyRangesAvg;
            this.candidatesAvg = candidatesAvg;
            this.finalResultCountAvg = finalResultCountAvg;
            this.visitedCellsAvg = visitedCellsAvg;
            this.redisAccessCountAvg = redisAccessCountAvg;
            this.redisShapeFilterRateScaledAvg = redisShapeFilterRateScaledAvg;
            this.indexSizeKb = indexSizeKb;
        }

        static MetricsRow from(Variant variant, ExperimentStats stats) {
            DatasetConfig dataset = stats.getDataset();
            int effectiveAlpha = variant.method == IndexMethod.LETI ? dataset.getLetiAlphaOrDefault() : dataset.getAlpha();
            int effectiveBeta = variant.method == IndexMethod.LETI ? dataset.getLetiBetaOrDefault() : dataset.getBeta();
            String orderPath;
            switch (variant.orderFamily) {
                case BMTREE:
                    orderPath = String.format(Locale.ROOT, "bmtree/%s/%s_order.json",
                            dataset.getDatasetName().toLowerCase(Locale.ROOT).replace('-', '_'),
                            normalizeDistributionStatic(dataset.getDistribution()));
                    break;
                case LMSFC:
                    orderPath = String.format(Locale.ROOT, "lmsfc/%s/%s_order.json",
                            dataset.getDatasetName().toLowerCase(Locale.ROOT).replace('-', '_'),
                            normalizeDistributionStatic(dataset.getDistribution()));
                    break;
                case LETI:
                default:
                    orderPath = String.format(Locale.ROOT, "leti/%s/param/quadorder_%s_res%d_min%d.json",
                            dataset.getDatasetName().toLowerCase(Locale.ROOT).replace('-', '_'),
                            dataset.getDistribution() == null ? "skewed" : dataset.getDistribution().toLowerCase(Locale.ROOT),
                            dataset.getResolution(),
                            dataset.getMinTraj());
                    break;
            }
            return new MetricsRow(
                    variant.label,
                    dataset.getDatasetName(),
                    "default",
                    dataset.getDistribution(),
                    dataset.getQueryRange(),
                    dataset.getResolution(),
                    effectiveAlpha,
                    effectiveBeta,
                    variant.adaptivePartition ? 1 : 0,
                    orderPath,
                    stats.getLatencyStats().getAvg(),
                    stats.getLogicIndexRangeStats().getAvg(),
                    stats.getQuadCodeRangeStats().getAvg(),
                    stats.getQOrderRangeStats().getAvg(),
                    stats.getRowKeyRangeStats().getAvg(),
                    stats.getCandidatesStats().getAvg(),
                    stats.getFinalResultStats().getAvg(),
                    stats.getVisitedCellsStats().getAvg(),
                    stats.getRedisAccessCountStats().getAvg(),
                    stats.getRedisShapeFilterRateScaledStats().getAvg(),
                    stats.getIndexSizeKb()
            );
        }

        private static String normalizeDistributionStatic(String distribution) {
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

        static String csv(String value) {
            String safe = value == null ? "" : value;
            if (safe.contains(",") || safe.contains("\"")) {
                return "\"" + safe.replace("\"", "\"\"") + "\"";
            }
            return safe;
        }

        String toCsv() {
            return String.join(",",
                    csv(variantLabel),
                    csv(datasetName),
                    csv(mode),
                    csv(distribution),
                    String.valueOf(queryRange),
                    String.valueOf(resolution),
                    String.valueOf(alpha),
                    String.valueOf(beta),
                    String.valueOf(adaptivePartition),
                    csv(orderPath),
                    String.valueOf(latencyAvg),
                    String.valueOf(logicalIndexRangesAvg),
                    String.valueOf(quadCodeRangesAvg),
                    String.valueOf(qOrderRangesAvg),
                    String.valueOf(rowKeyRangesAvg),
                    String.valueOf(candidatesAvg),
                    String.valueOf(finalResultCountAvg),
                    String.valueOf(visitedCellsAvg),
                    String.valueOf(redisAccessCountAvg),
                    String.valueOf(redisShapeFilterRateScaledAvg),
                    String.valueOf(indexSizeKb));
        }
    }

    public static void main(String[] args) {
        if (args.length < 4) {
            System.out.println("Usage: AblationExperiment <query_base_dir> <data_base_dir> <output_dir> <dataset_name>");
            return;
        }
        new AblationExperiment(args[0], args[1], args[2], args[3]).run();
    }
}
