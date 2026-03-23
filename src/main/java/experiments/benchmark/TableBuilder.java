package experiments.benchmark;

import com.esri.core.geometry.Envelope;
import config.TableConfig;
import constans.IndexEnum;
import loader.Loader;
import loader.LetiLoader;
import preprocess.compress.IIntegerCompress;

import java.io.IOException;

/**
 * 索引表创建工具
 * 根据不同方法使用对应的Loader创建HBase表
 */
public class TableBuilder {

    private final String sourcePath;
    private final String resultPath;

    public TableBuilder(String sourcePath, String resultPath) {
        this.sourcePath = sourcePath;
        this.resultPath = resultPath;
    }

    /**
     * 构建表配置 - 使用DatasetConfig中的默认值
     *
     * @param config DatasetConfig配置对象
     * @return TableConfig
     */
    public static TableConfig buildConfig(DatasetConfig config) {
        return buildConfig(config.getDatasetName(), config.getResolution(), config.getAlpha(), config.getBeta());
    }

    /**
     * 构建表配置 - 自动根据数据集选择空间范围
     *
     * @param datasetName 数据集名称 (tdrive 或 cd-taxi)
     * @param resolution  分辨率
     * @param alpha       网格宽度
     * @param beta        网格高度
     * @return TableConfig
     */
    public static TableConfig buildConfig(String datasetName, int resolution, int alpha, int beta) {
        double xmin, ymin, xmax, ymax;

        if (datasetName.equalsIgnoreCase(DatasetConfig.T_DRIVE)) {
            // TDrive数据集范围
            xmin = 115.29;
            ymin = 39.00;
            xmax = 117.83;
            ymax = 41.50;
        } else if (datasetName.equalsIgnoreCase(DatasetConfig.CD_TAXI)) {
            // CD数据集范围
            xmin = 104.04;
            ymin = 30.65;
            xmax = 104.13;
            ymax = 30.73;
        } else {
            throw new IllegalArgumentException("Unknown dataset: " + datasetName + ". Supported: tdrive, cd");
        }

        return buildConfigInternal(resolution, alpha, beta, xmin, ymin, xmax, ymax);
    }

    /**
     * 构建表配置
     */
    private static TableConfig buildConfigInternal(int resolution, int alpha, int beta,
                                                   double xmin, double ymin, double xmax, double ymax) {
        TableConfig config = new TableConfig(
                IndexEnum.INDEX_TYPE.SPATIAL,
                resolution, alpha, beta,
                1.0, 24,  // timeBin, timeBinNums
                IIntegerCompress.CompressType.DELTA_COMPRESS_VGB,
                new Envelope(xmin, ymin, xmax, ymax),
                (short) 4  // shards
        );
        config.setRedisHost("127.0.0.1");
        return config;
    }

    /**
     * 创建索引表
     *
     * @param method    索引方法
     * @param tableName 表名
     * @param config    表配置
     * @throws IOException 建表异常
     */
    public void createTable(IndexMethod method, String tableName, TableConfig config) throws IOException {
        System.out.println("\n" + repeatString());
        System.out.println("Creating table for: " + method.getShortName());
        System.out.println("Table name: " + tableName);
        System.out.println(repeatString());

        config.setTableName(tableName);

        switch (method) {
            case LETI:
                createLetiTable(config);
                break;
            case TSHAPE:
                createTShapeTable(config);
                break;
            case XZ_STAR:
                createXZStarTable(config);
                break;
            case LMSFC:
            case BMTREE:
                throw new UnsupportedOperationException("Method " + method + " not yet implemented");
            default:
                throw new IllegalArgumentException("Unknown method: " + method);
        }

        System.out.println("Table creation completed: " + tableName);
    }

    /**
     * 重复字符串n次
     */
    private static String repeatString() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 60; i++) {
            sb.append("=");
        }
        return sb.toString();
    }

    /**
     * 创建LETI表 - 使用LetiLoader
     * 配置: isXZ=0, rlEncoding=1, tspEncoding=1
     */
    private void createLetiTable(TableConfig config) throws IOException {
        System.out.println("Using LetiLoader...");
        System.out.println("Config: isXZ=0, rlEncoding=1, tspEncoding=1, adaptivePartition=1");

        config.setRlEncoding(1);
        config.setIsXZ(0);
        config.setTspEncoding(1);
        config.setAdaptivePartition(1);

        try (LetiLoader loader = new LetiLoader(config, sourcePath, resultPath)) {
            loader.store();
        }
    }

    /**
     * 创建TShape表 - 使用标准Loader
     * 配置: isXZ=0, tspEncoding=1
     */
    private void createTShapeTable(TableConfig config) throws IOException {
        System.out.println("Using Loader (TShape mode)...");
        System.out.println("Config: isXZ=0, tspEncoding=1");

        config.setIsXZ(0);
        config.setTspEncoding(1);

        try (Loader loader = new Loader(config, sourcePath, resultPath)) {
            loader.store();
        }
    }

    /**
     * 创建 XZ_STAR 表：
     * - 固定使用 2*2（alpha/beta=2）
     * - 不使用 tspEncoding（与 XZStarSFC 适配，当前版本只实现 SRQ ranges）
     * - 使用现有 Loader 写入主表/元数据
     */
    private void createXZStarTable(TableConfig config) throws IOException {
        System.out.println("Using XZStarIndex (fixed alpha=2,beta=2, tspEncoding=0)...");

        config.setIsXZ(1);
        config.setTspEncoding(0);
        config.setAlpha(2);
        config.setBeta(2);

        try (Loader loader = new Loader(config, sourcePath, resultPath)) {
            loader.store();
        }
    }
}
