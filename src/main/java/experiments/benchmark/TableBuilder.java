package experiments.benchmark;

import com.esri.core.geometry.Envelope;
import config.TableConfig;
import constans.IndexEnum;
import loader.Loader;
import loader.LMSFCLoader;
import loader.BMTreeLoader;
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
                createLMSFCTable(config);
                break;
            case BMTREE:
                createBMTreeTable(config);
                break;
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

    /**
     * 创建 LMSFC 表：
     * - 使用 LMSFCLoader
     * - 设置 thetaConfig（θ参数配置）
     * - 使用 LMSFC 索引策略
     */
    private void createLMSFCTable(TableConfig config) throws IOException {
        System.out.println("Using LMSFCLoader...");

        // 从 config 获取 thetaConfig，如果没有则使用默认值
        String thetaConfig = config.getThetaConfig();
        if (thetaConfig == null || thetaConfig.isEmpty()) {
            // 默认 θ 配置（20个层级，可根据需要调整）
            thetaConfig = "0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19";
            System.out.println("Using default thetaConfig: " + thetaConfig);
        }

        System.out.println("Config: isLMSFC=1, thetaConfig=" + thetaConfig);

        config.setIsLMSFC(1);
        config.setThetaConfig(thetaConfig);

        try (LMSFCLoader loader = new LMSFCLoader(config, sourcePath, resultPath)) {
            loader.store();
        }
    }

    /**
     * 创建 BMTree 表：
     * - 使用 BMTreeLoader
     * - 设置 bmtreeConfigPath（配置文件路径）
     * - 设置 bmtreeBitLength（bit长度配置）
     * - 使用 BMTree 索引策略
     */
    private void createBMTreeTable(TableConfig config) throws IOException {
        System.out.println("Using BMTreeLoader...");

        // 从 config 获取 BMTree 配置
        String bmtreeConfigPath = config.getBMTreeConfigPath();
        int[] bitLength = config.getBMTreeBitLength(); // 注意：这里返回的是 int[]

        // 验证必需的配置
        if (bmtreeConfigPath == null || bmtreeConfigPath.isEmpty()) {
            throw new IllegalArgumentException("BMTreeConfigPath is required for BMTree index");
        }

        // 将 int[] 转换为字符串（参考 BMTreeLoader 的做法）
        String bmtreeBitLength;
        if (bitLength == null || bitLength.length == 0) {
            // 默认 bit 长度配置
            bmtreeBitLength = "20,20";
            System.out.println("Using default bmtreeBitLength: " + bmtreeBitLength);
        } else {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < bitLength.length; i++) {
                sb.append(bitLength[i]);
                if (i < bitLength.length - 1) {
                    sb.append(",");
                }
            }
            bmtreeBitLength = sb.toString();
        }

        System.out.println("Config: isBMTree=1, configPath=" + bmtreeConfigPath +
                ", bitLength=" + bmtreeBitLength);

        config.setIsBMTree(1);
        config.setBMTreeConfigPath(bmtreeConfigPath);
        config.setBMTreeBitLength(bmtreeBitLength); // 设置字符串格式

        try (BMTreeLoader loader = new BMTreeLoader(config, sourcePath, resultPath)) {
            loader.store();
        }
    }













}
