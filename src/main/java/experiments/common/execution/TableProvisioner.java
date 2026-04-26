package experiments.common.execution;

import config.TableConfig;
import experiments.common.config.DatasetConfig;
import experiments.common.config.IndexMethod;
import experiments.common.io.BenchmarkTableCleaner;
import experiments.common.io.TableBuilder;
import experiments.suite.comparison.model.ExperimentCase;
import lombok.Getter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import utils.LetiOrderResolver;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

public class TableProvisioner {
    private final String outputDir;
    private final Map<String, TableHandle> ensuredTables = new LinkedHashMap<>();
    private final Set<String> createdTables = new LinkedHashSet<>();

    public TableProvisioner(String outputDir) {
        this.outputDir = outputDir;
    }

    public TableHandle ensure(ExperimentCase experimentCase) {
        DatasetConfig datasetConfig = toDatasetConfig(experimentCase);
        String tableName = buildTableName(experimentCase, datasetConfig);
        TableHandle existing = ensuredTables.get(tableName);
        if (existing != null) {
            return existing;
        }

        TableConfig tableConfig = buildTableConfig(experimentCase, datasetConfig);
        recreateTable(experimentCase.getMethod(), datasetConfig, tableName, tableConfig);

        TableHandle handle = new TableHandle(tableName, tableConfig);
        ensuredTables.put(tableName, handle);
        createdTables.add(tableName);
        return handle;
    }

    public void cleanup() {
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
            System.err.println("Error cleaning comparison tables: " + e.getMessage());
        }
    }

    private void recreateTable(IndexMethod method, DatasetConfig datasetConfig, String tableName, TableConfig tableConfig) {
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
                builder.createTable(method, tableName, tableConfig);
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to prepare table " + tableName, e);
        }
    }

    private DatasetConfig toDatasetConfig(ExperimentCase experimentCase) {
        DatasetConfig datasetConfig = new DatasetConfig(
                experimentCase.getDataset().getName(),
                experimentCase.getDistribution(),
                experimentCase.getQueryRange());
        datasetConfig.setQueryBaseDir(experimentCase.getDataset().getQueryBaseDir());
        datasetConfig.setDataFilePath(experimentCase.getDataset().getDataFilePath());
        if (experimentCase.getDataset().getResolution() != null) {
            datasetConfig.setResolution(experimentCase.getDataset().getResolution());
        }
        if (experimentCase.getDataset().getMinTraj() != null) {
            datasetConfig.setMinTraj(experimentCase.getDataset().getMinTraj());
        }
        if (experimentCase.getDataset().getAlpha() != null) {
            datasetConfig.setAlpha(experimentCase.getDataset().getAlpha());
        }
        if (experimentCase.getDataset().getBeta() != null) {
            datasetConfig.setBeta(experimentCase.getDataset().getBeta());
        }
        if (experimentCase.getDataset().getLetiAlpha() != null) {
            datasetConfig.setLetiAlpha(experimentCase.getDataset().getLetiAlpha());
        }
        if (experimentCase.getDataset().getLetiBeta() != null) {
            datasetConfig.setLetiBeta(experimentCase.getDataset().getLetiBeta());
        }
        if (experimentCase.getDataset().getNodes() != null) {
            datasetConfig.setNodes(experimentCase.getDataset().getNodes());
        }
        if (experimentCase.getDataset().getShards() != null) {
            datasetConfig.setShards(experimentCase.getDataset().getShards());
        }
        if (experimentCase.getDataset().getQueryType() != null && !experimentCase.getDataset().getQueryType().isEmpty()) {
            datasetConfig.setQueryType(experimentCase.getDataset().getQueryType());
        }
        if (experimentCase.getDataset().getThetaConfig() != null && !experimentCase.getDataset().getThetaConfig().isEmpty()) {
            datasetConfig.setThetaConfig(experimentCase.getDataset().getThetaConfig());
        }
        if (experimentCase.getDataset().getThetaConfigPath() != null && !experimentCase.getDataset().getThetaConfigPath().isEmpty()) {
            datasetConfig.setThetaConfigPath(experimentCase.getDataset().getThetaConfigPath());
        }
        if (experimentCase.getDataset().getBmtreeConfigPath() != null && !experimentCase.getDataset().getBmtreeConfigPath().isEmpty()) {
            datasetConfig.setBmtreeConfigPath(experimentCase.getDataset().getBmtreeConfigPath());
        }
        if (experimentCase.getDataset().getBmtreeBitLength() != null && !experimentCase.getDataset().getBmtreeBitLength().isEmpty()) {
            datasetConfig.setBmtreeBitLength(experimentCase.getDataset().getBmtreeBitLength());
        }
        return datasetConfig;
    }

    private TableConfig buildTableConfig(ExperimentCase experimentCase, DatasetConfig datasetConfig) {
        TableConfig tableConfig = TableBuilder.buildConfig(datasetConfig);
        switch (experimentCase.getMethod()) {
            case LETI:
                tableConfig.setAdaptivePartition(1);
                tableConfig.setAlpha(datasetConfig.getLetiAlphaOrDefault());
                tableConfig.setBeta(datasetConfig.getLetiBetaOrDefault());
                tableConfig.setOrderDefinitionPath(resolveLetiOrderingPath(datasetConfig));
                tableConfig.setOrderEncodingType(1);
                tableConfig.setTspEncoding(1);
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
            case LMSFC:
                tableConfig.setIsLMSFC(1);
                tableConfig.setThetaConfig(datasetConfig.getThetaConfigOrDefault());
                break;
            case BMTREE:
                tableConfig.setIsBMTree(1);
                tableConfig.setBMTreeConfigPath(datasetConfig.getBMTreeConfigPathOrDefault());
                tableConfig.setBMTreeBitLength(datasetConfig.getBMTreeBitLengthOrDefault());
                break;
            default:
                break;
        }
        return tableConfig;
    }

    private String resolveLetiOrderingPath(DatasetConfig datasetConfig) {
        String datasetKey = normalizeDataset(datasetConfig.getDatasetName());
        String distribution = datasetConfig.getDistribution().toLowerCase(Locale.ROOT);
        String distributionKey = normalizeDistribution(datasetConfig.getDistribution());
        int resolution = datasetConfig.getResolution();
        int minTraj = datasetConfig.getMinTraj();
        int letiAlpha = datasetConfig.getLetiAlphaOrDefault();
        int letiBeta = datasetConfig.getLetiBetaOrDefault();

        String[] candidates = new String[] {
                String.format(Locale.ROOT, "leti/%s/comparison/%s_r%d_min%d_a%d_b%d.json",
                        datasetKey, distribution, resolution, minTraj, letiAlpha, letiBeta),
                String.format(Locale.ROOT, "leti/%s/comparison/%s_order.json", datasetKey, distributionKey),
                String.format(Locale.ROOT, "leti/%s/param/%s_r%d_min%d_a%d_b%d.json",
                        datasetKey, distribution, resolution, minTraj, letiAlpha, letiBeta)
        };
        for (String resource : candidates) {
            if (TableProvisioner.class.getClassLoader().getResource(resource) != null) {
                return LetiOrderResolver.resolveClasspathResource(resource);
            }
        }
        return LetiOrderResolver.defaultOrderPath();
    }

    private String buildTableName(ExperimentCase experimentCase, DatasetConfig datasetConfig) {
        String datasetToken = normalizeDataset(experimentCase.getDataset().getName());
        String methodToken = experimentCase.getMethod().getShortName().toLowerCase(Locale.ROOT);
        if (experimentCase.getMethod() == IndexMethod.LETI) {
            return String.format(Locale.ROOT, "%s_%s_%s_r%d_m%d_a%d_b%d",
                    datasetToken,
                    methodToken,
                    experimentCase.getDistribution().toLowerCase(Locale.ROOT),
                    datasetConfig.getResolution(),
                    datasetConfig.getMinTraj(),
                    datasetConfig.getLetiAlphaOrDefault(),
                    datasetConfig.getLetiBetaOrDefault());
        }
        if (experimentCase.getMethod() == IndexMethod.BMTREE) {
            return String.format(Locale.ROOT, "%s_%s_r%d_bits_%s",
                    datasetToken,
                    methodToken,
                    datasetConfig.getResolution(),
                    datasetConfig.getBMTreeBitLengthOrDefault().replace(',', '_'));
        }
        if (experimentCase.getMethod() == IndexMethod.LMSFC) {
            return String.format(Locale.ROOT, "%s_%s_r%d",
                    datasetToken,
                    methodToken,
                    datasetConfig.getResolution());
        }
        return String.format(Locale.ROOT, "%s_%s", datasetToken, methodToken);
    }

    private String normalizeDataset(String value) {
        return value == null ? "unknown" : value.trim().toLowerCase(Locale.ROOT).replace('-', '_');
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

    @Getter
    public static class TableHandle {
        private final String tableName;
        private final TableConfig tableConfig;

        public TableHandle(String tableName, TableConfig tableConfig) {
            this.tableName = tableName;
            this.tableConfig = tableConfig;
        }
    }
}
