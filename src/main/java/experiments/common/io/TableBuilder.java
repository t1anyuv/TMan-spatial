package experiments.common.io;

import com.esri.core.geometry.Envelope;
import config.TableConfig;
import constans.IndexEnum;
import experiments.common.config.DatasetConfig;
import experiments.common.config.IndexMethod;
import loader.BMTreeLoader;
import loader.LMSFCLoader;
import loader.LetiLoader;
import loader.Loader;
import loader.Loader.StoreSummary;
import preprocess.compress.IIntegerCompress;
import utils.BMTreeConfigLearner;

import java.io.IOException;

public class TableBuilder {

    private final String sourcePath;
    private final String resultPath;
    private final String queryWorkloadPath;

    public TableBuilder(String sourcePath, String resultPath) {
        this(sourcePath, resultPath, null);
    }

    public TableBuilder(String sourcePath, String resultPath, String queryWorkloadPath) {
        this.sourcePath = sourcePath;
        this.resultPath = resultPath;
        this.queryWorkloadPath = queryWorkloadPath;
    }

    public static TableConfig buildConfig(DatasetConfig config) {
        return buildConfig(config.getDatasetName(), config.getResolution(), config.getAlpha(), config.getBeta());
    }

    public static TableConfig buildConfig(String datasetName, int resolution, int alpha, int beta) {
        double xmin, ymin, xmax, ymax;
        if (datasetName.equalsIgnoreCase(DatasetConfig.T_DRIVE)) {
            xmin = 115.29; ymin = 39.00; xmax = 117.83; ymax = 41.50;
        } else if (datasetName.equalsIgnoreCase(DatasetConfig.CD_TAXI)) {
            xmin = 104.04; ymin = 30.65; xmax = 104.13; ymax = 30.73;
        } else {
            throw new IllegalArgumentException("Unknown dataset: " + datasetName + ". Supported: tdrive, cd");
        }
        TableConfig config = new TableConfig(
                IndexEnum.INDEX_TYPE.SPATIAL,
                resolution, alpha, beta,
                1.0, 24,
                IIntegerCompress.CompressType.DELTA_COMPRESS_VGB,
                new Envelope(xmin, ymin, xmax, ymax),
                (short) 4
        );
        config.setRedisHost("127.0.0.1");
        return config;
    }

    public StoreSummary createTable(IndexMethod method, String tableName, TableConfig config) throws IOException {
        config.setTableName(tableName);
        switch (method) {
            case LETI:
                return createLetiTable(config);
            case TSHAPE:
                return createTShapeTable(config);
            case XZ_STAR:
                return createXZStarTable(config);
            case LMSFC:
                return createLMSFCTable(config);
            case BMTREE:
                return createBMTreeTable(config);
            default:
                throw new IllegalArgumentException("Unknown method: " + method);
        }
    }

    private StoreSummary createLetiTable(TableConfig config) throws IOException {
        config.setOrderEncodingType(1);
        config.setIsXZ(0);
        config.setTspEncoding(1);
        try (LetiLoader loader = new LetiLoader(config, sourcePath, resultPath)) {
            loader.store();
            return loader.getLastStoreSummary();
        }
    }

    private StoreSummary createTShapeTable(TableConfig config) throws IOException {
        config.setIsXZ(0);
        config.setTspEncoding(1);
        try (Loader loader = new Loader(config, sourcePath, resultPath)) {
            loader.store();
            return loader.getLastStoreSummary();
        }
    }

    private StoreSummary createXZStarTable(TableConfig config) throws IOException {
        config.setIsXZ(1);
        config.setTspEncoding(0);
        config.setAlpha(2);
        config.setBeta(2);
        try (Loader loader = new Loader(config, sourcePath, resultPath)) {
            loader.store();
            return loader.getLastStoreSummary();
        }
    }

    private StoreSummary createLMSFCTable(TableConfig config) throws IOException {
        String thetaConfig = config.getThetaConfig();
        if (thetaConfig == null || thetaConfig.isEmpty()) {
            thetaConfig = "0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19";
        }
        config.setIsLMSFC(1);
        config.setThetaConfig(thetaConfig);
        try (LMSFCLoader loader = new LMSFCLoader(config, sourcePath, resultPath)) {
            loader.store();
            return loader.getLastStoreSummary();
        }
    }

    private StoreSummary createBMTreeTable(TableConfig config) throws IOException {
        String bmtreeConfigPath = config.getBMTreeConfigPath();
        int[] bitLength = config.getBMTreeBitLength();
        if (bmtreeConfigPath == null || bmtreeConfigPath.isEmpty()) {
            throw new IllegalArgumentException("BMTreeConfigPath is required for BMTree index");
        }
        String bmtreeBitLength;
        if (bitLength == null || bitLength.length == 0) {
            bmtreeBitLength = "20,20";
        } else {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < bitLength.length; i++) {
                sb.append(bitLength[i]);
                if (i < bitLength.length - 1) sb.append(",");
            }
            bmtreeBitLength = sb.toString();
        }
        config.setIsBMTree(1);
        int[] safeBitLength = bitLength == null || bitLength.length == 0 ? new int[]{20, 20} : bitLength;
        String ensuredConfigPath;
        if (queryWorkloadPath != null && !queryWorkloadPath.trim().isEmpty()) {
            ensuredConfigPath = BMTreeConfigLearner.ensureLearnedConfigExists(
                    bmtreeConfigPath,
                    safeBitLength,
                    config.getResolution(),
                    sourcePath,
                    queryWorkloadPath,
                    BMTreeConfigLearner.DEFAULT_TRAJECTORY_SAMPLE_LIMIT,
                    BMTreeConfigLearner.DEFAULT_QUERY_SAMPLE_LIMIT,
                    BMTreeConfigLearner.DEFAULT_PAGE_SIZE,
                    BMTreeConfigLearner.DEFAULT_MIN_NODE_SIZE,
                    BMTreeConfigLearner.DEFAULT_SEED);
        } else {
            ensuredConfigPath = BMTreeConfigLearner.ensureConfigExists(
                    bmtreeConfigPath,
                    safeBitLength,
                    config.getResolution());
        }
        config.setBMTreeConfigPath(ensuredConfigPath);
        config.setBMTreeBitLength(bmtreeBitLength);
        try (BMTreeLoader loader = new BMTreeLoader(config, sourcePath, resultPath)) {
            loader.store();
            return loader.getLastStoreSummary();
        }
    }
}
