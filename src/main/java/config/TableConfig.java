package config;

import com.esri.core.geometry.Envelope;
import constans.IndexEnum;
import preprocess.compress.IIntegerCompress;
import lombok.Getter;
import lombok.Setter;
import scala.Enumeration;

import java.io.Serializable;


@Getter
@Setter
public class TableConfig implements Serializable {
    private int adaptivePartition = 0;

    @Getter
    private IndexEnum.INDEX_TYPE primary;

    private int resolution;
    private int alpha;
    private int beta;
    private double timeBin;
    private int timeBinNums;
    private String redisHost;
    private IIntegerCompress.CompressType compressType;
    private Enumeration.Value timePeriod;

    private Envelope envelope;

    @Setter
    @Getter
    private String tableName;
    private Short shards;
    private int isXZ = 0;
    private int tspEncoding = 0;
    private int orderEncodingType = 0;
    private String orderDefinitionPath;

    /**
     * XZ_Plus 模式识别。
     * <p>
     * <p>
     * - isXZPlus=1 的含义：
     * 1) isXZ=1
     * 2) alpha=beta
     * 3) tspEncoding=0（不参与 TSP 编码）
     * 4) 且不是 XZ_STAR（由 {@link #isXZStar()} 决定）
     * <p>
     * 注意：在 {@link #getSpatialIndexKind()} 中，会先判断更高优先级（BMTree/LMSFC/LETI/XZ_STAR）。
     */
    public boolean isXZPlus() {
        return isXZ == 1 && alpha == beta && tspEncoding == 0 && !isXZStar();
    }

    /**
     * 自适应划分时的最大形状位数
     * 等于所有 ParentQuad 中 alpha * beta 的最大值
     * 用于统一编码空间，避免不同 quad 的索引范围重叠
     */
    private int mainTableMoveBits = 0;

    private int isLMSFC = 0;
    private String thetaConfig = "";
    private int isBMTree = 0;
    private String bmtreeConfigPath = "";
    private int[] bmtreeBitLength = new int[0];
    private String letiOrderName = "";
    private String letiOrderDataset = "";
    private String letiOrderDistribution = "";
    private String letiOrderVersion = "";
    private long letiOrderCount = 0L;
    private long letiActiveCells = 0L;
    private long letiTotalCells = 0L;
    private int letiMaxLevel = 0;
    private int letiGlobalAlpha = 0;
    private int letiGlobalBeta = 0;
    private Envelope letiOrderBoundary;

    public TableConfig() {
    }

    public TableConfig(IndexEnum.INDEX_TYPE primary, int resolution, int alpha, int beta, double timeBin, int timeBinNums, IIntegerCompress.CompressType compressType) {
        this(primary, resolution, alpha, beta, timeBin, timeBinNums, compressType, null, null, null);
    }

    public TableConfig(IndexEnum.INDEX_TYPE primary, int resolution, int alpha, int beta, double timeBin, int timeBinNums, IIntegerCompress.CompressType compressType, Envelope envelope) {
        this(primary, resolution, alpha, beta, timeBin, timeBinNums, compressType, envelope, null, null);
    }

    public TableConfig(IndexEnum.INDEX_TYPE primary, int resolution, int alpha, int beta, double timeBin, int timeBinNums, IIntegerCompress.CompressType compressType, Envelope envelope, Short shards) {
        this(primary, resolution, alpha, beta, timeBin, timeBinNums, compressType, envelope, shards, null);
    }

    public TableConfig(IndexEnum.INDEX_TYPE primary, int resolution, int alpha, int beta, double timeBin, int timeBinNums, IIntegerCompress.CompressType compressType, Enumeration.Value timePeriod) {
        this(primary, resolution, alpha, beta, timeBin, timeBinNums, compressType, null, null, timePeriod);
    }

    public TableConfig(IndexEnum.INDEX_TYPE primary, int resolution, int alpha, int beta, double timeBin, int timeBinNums, IIntegerCompress.CompressType compressType, Envelope envelope, Short shards, Enumeration.Value timePeriod) {
        this.primary = primary;
        this.resolution = resolution;
        this.alpha = alpha;
        this.beta = beta;
        this.timeBin = timeBin;
        this.timeBinNums = timeBinNums;
        this.compressType = compressType;
        this.envelope = envelope;
        this.shards = shards;
        this.timePeriod = timePeriod;
    }

    public String getRedisHost() {
        if (null == redisHost) {
            return "127.0.0.1";
        }
        return redisHost;
    }

    /**
     * TSP 编码是否启用。
     * <p>
     *
     * - LETI 模式要求 tspEncoding=1/2（两种方式分别对应 1 和 2）
     *
     * @return boolean
     *
     */
    public boolean isTspEncoding() {
        return tspEncoding == 1 || tspEncoding == 2;
    }

    /**
     * LETI 模式识别。
     * <p>
     *
     * - orderEncodingType=1
     * - 且满足 isXZPlus=0
     * - 且 tspEncoding=1/2（由 {@link #isTspEncoding()} 判断）
     * <p>
     * 注意：在 {@link #getSpatialIndexKind()} 中，会先判断更高优先级（BMTree/LMSFC）。
     *
     * @return boolean
     */
    public boolean isLETI() {
        return orderEncodingType == 1 && !isXZPlus() && isTspEncoding();
    }

    /**
     * 是否使用自适应划分。
     * <p>
     * 该开关仅在当前空间索引模式为 LETI 时生效（即 {@link #isLETI()} 为 true），并且
     * {@code adaptivePartition=1} 才会启用。
     *
     * @return boolean
     */
    public boolean isAdaptivePartition() {
        return isLETI() && adaptivePartition == 1;
    }

    /**
     * XZ_STAR 模式识别。
     * <p>
     * - isXZ=1
     * - alpha=2, beta=2
     * - tspEncoding=0（不参与 TSP 编码）
     * <p>
     * 注意：在 {@link #getSpatialIndexKind()} 中，XZ_STAR 的优先级高于 XZ_Plus。
     */
    public boolean isXZStar() {
        return isXZ == 1
                && alpha == 2
                && beta == 2
                && tspEncoding == 0;
    }

    /**
     * LMSFC 模式识别。
     * <p>
     * - isLMSFC=1
     * <p>
     * 注意：在 {@link #getSpatialIndexKind()} 中，BMTree 的优先级高于 LMSFC。
     *
     * @return boolean
     */
    public boolean isLMSFC() {
        return isLMSFC == 1;
    }

    /**
     * BMTree 模式识别。
     * <p>
     * 规则对应你给的“六种空间索引”：
     * - isBMTree=1
     * <p>
     * 注意：在 {@link #getSpatialIndexKind()} 中，该模式优先级最高。
     *
     * @return boolean
     */
    public boolean isBMTree() {
        return isBMTree == 1;
    }

    /**
     * 当前配置对应的“六种空间索引”之一。
     * 规则（优先级从高到低）：
     * - BMTree: isBMTree=1
     * - LMSFC: isLMSFC=1
     * - LETI: orderEncodingType=1, isXZPlus=0, 且 tspEncoding=1/2
     * - XZ_STAR: isXZPlus=1, alpha=2, beta=2, tspEncoding=0
     * - XZ_Plus: isXZPlus=1, alpha=beta, tspEncoding=0, not XZ_STAR
     * - TShape:
     */
    public SpatialIndexKind getSpatialIndexKind() {
        if (isBMTree()) {
            return SpatialIndexKind.BMTREE;
        }
        if (isLMSFC()) {
            return SpatialIndexKind.LMSFC;
        }
        if (isLETI()) {
            return SpatialIndexKind.LETI;
        }
        if (isXZStar()) {
            return SpatialIndexKind.XZ_STAR;
        }
        if (isXZPlus()) {
            return SpatialIndexKind.XZPlus;
        }
        return SpatialIndexKind.TShape;
    }

    /**
     * 获取 BMTree 配置文件路径
     */
    public String getBMTreeConfigPath() {
        return bmtreeConfigPath;
    }

    /**
     * 设置 BMTree 配置文件路径
     */
    public void setBMTreeConfigPath(String bmtreeConfigPath) {
        this.bmtreeConfigPath = bmtreeConfigPath;
    }

    /**
     * 获取 BMTree bit长度配置（int数组）
     */
    public int[] getBMTreeBitLength() {
        return bmtreeBitLength;
    }

    /**
     * 设置 BMTree bit长度配置（接受字符串，如"20,20"）
     */
    public void setBMTreeBitLength(String bitLengthStr) {
        if (bitLengthStr == null || bitLengthStr.trim().isEmpty()) {
            this.bmtreeBitLength = new int[] { 20, 20 }; // 默认值
            return;
        }
        String[] parts = bitLengthStr.split(",");
        this.bmtreeBitLength = new int[parts.length];
        for (int i = 0; i < parts.length; i++) {
            this.bmtreeBitLength[i] = Integer.parseInt(parts[i].trim());
        }
    }

    /**
     * 设置 BMTree bit长度配置（接受int数组）
     */
    public void setBMTreeBitLength(int[] bitLength) {
        this.bmtreeBitLength = bitLength;
    }

    public enum SpatialIndexKind {
        TShape,
        XZPlus,
        LETI,
        XZ_STAR,
        LMSFC,
        BMTREE
    }
}
