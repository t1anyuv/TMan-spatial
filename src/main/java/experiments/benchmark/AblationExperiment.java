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
            Map<String, String> latencyByMethod = new LinkedHashMap<>();

            for (Variant variant : variants()) {
                String tableName = buildTableName(variant);
                ensureTable(variant, config, tableName);
                ExperimentStats stats = runner.runBenchmark(IndexMethod.LETI, config, tableName);
                latencyByMethod.put(variant.label, String.valueOf(stats.getLatencyStats().getAvg()));
            }

            writeAblationCsv(latencyByMethod);
        } catch (IOException e) {
            throw new RuntimeException("Failed to run ablation experiment", e);
        } finally {
            cleanupTables();
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

    private List<Variant> variants() {
        return Arrays.asList(
                new Variant("LETI", true, null),
                new Variant("LETI(LMSFC)", true, "lmsfc/tdrive/uni_order.json"),
                new Variant("LETI(LBMT)", true, "bmtree/tdrive/uni_order.json"),
                new Variant("LETI-ab", false, null)
        );
    }

    private void ensureTable(Variant variant, DatasetConfig dataset, String tableName) {
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
                config.setAdaptivePartition(variant.adaptivePartition ? 1 : 0);
                config.setOrderDefinitionPath(resolveOrderingPath(dataset, variant));

                TableBuilder builder = new TableBuilder(dataset.getDataFilePath(), outputDir);
                builder.createTable(IndexMethod.LETI, tableName, config);
                createdTables.add(tableName);
            }
        } catch (Exception e) {
            throw new RuntimeException("Error creating table " + tableName, e);
        }
    }

    private String resolveOrderingPath(DatasetConfig dataset, Variant variant) {
        if (variant.explicitOrderingPath != null && !variant.explicitOrderingPath.trim().isEmpty()) {
            return LetiOrderResolver.resolveClasspathResource(variant.explicitOrderingPath);
        }
        String candidate = String.format(Locale.ROOT, "leti/%s/%s_order.json",
                dataset.getDatasetName().toLowerCase(Locale.ROOT).replace('-', '_'),
                normalizeDistribution(dataset.getDistribution()));
        ClassLoader classLoader = AblationExperiment.class.getClassLoader();
        if (classLoader.getResource(candidate) != null) {
            return LetiOrderResolver.resolveClasspathResource(candidate);
        }
        return LetiOrderResolver.defaultOrderPath();
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

    private String buildTableName(Variant variant) {
        return String.format(Locale.ROOT, "%s_ablation_%s",
                datasetName.toLowerCase(Locale.ROOT).replace("-", "_"),
                variant.label.toLowerCase(Locale.ROOT)
                        .replace("(", "_")
                        .replace(")", "")
                        .replace("-", "_"));
    }

    private void writeAblationCsv(Map<String, String> latencyByMethod) throws IOException {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(Paths.get(outputDir, "ablation_latency.csv").toFile()))) {
            writer.write("Dataset");
            for (Variant variant : variants()) {
                writer.write("," + variant.label);
            }
            writer.newLine();
            writer.write(datasetName);
            for (Variant variant : variants()) {
                writer.write("," + latencyByMethod.getOrDefault(variant.label, ""));
            }
            writer.newLine();
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
            System.err.println("Error cleaning ablation tables: " + e.getMessage());
        }
    }

    private static class Variant {
        final String label;
        final boolean adaptivePartition;
        final String explicitOrderingPath;

        Variant(String label, boolean adaptivePartition, String explicitOrderingPath) {
            this.label = label;
            this.adaptivePartition = adaptivePartition;
            this.explicitOrderingPath = explicitOrderingPath;
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
