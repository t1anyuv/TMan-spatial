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
    public enum SpatialIndexKind {
        LOC_S,
        XZ_LOC_S,
        LETI_LOC_S,
        XZ_STAR
    }

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
    private int rlEncoding = 0;
    private String rlOrderingPath;

    /**
     * 是否使用自适应划分：
     * 0 - 使用全局的 alpha 和 beta（默认）
     * 1 - 使用 ParentQuad 中的 alpha 和 beta（自适应）
     */
    private int adaptivePartition = 0;

    /**
     * 自适应划分时的最大形状位数
     * 等于所有 ParentQuad 中 alpha * beta 的最大值
     * 用于统一编码空间，避免不同 quad 的索引范围重叠
     */
    private int maxShapeBits = 0;

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

    public boolean isXZ() {
        return isXZ == 1;
    }

    /**
     * TSP 编码的两种方式：蚁群算法和遗传算法，分别对应 1 和 2
     *
     * @return boolean
     */
    public boolean isTspEncoding() {
        return tspEncoding == 1 || tspEncoding == 2;
    }

    /**
     * LETI 索引模式
     *
     * @return boolean
     */
    public boolean isLETI() {
        return rlEncoding == 1 && !isXZ() && isTspEncoding();
    }

    /**
     * 是否使用自适应划分
     *
     * @return boolean
     */
    public boolean isAdaptivePartition() {
        return isLETI() && adaptivePartition == 1;
    }

    /**
     * XZ_STAR 索引模式识别：
     * - 固定使用 alpha=2, beta=2
     * - 不参与 TSP 编码（tspEncoding=0）
     * - 且使用 XZ 编码路径（isXZ=1）
     */
    public boolean isXZStar() {
        return isXZ == 1
                && alpha == 2
                && beta == 2
                && tspEncoding == 0;
    }

    /**
     * 当前配置对应的“四种空间索引”之一。
     * 规则（优先级从高到低）：
     * - LETI: rlEncoding=1, isXZ=0, 且 tspEncoding=1/2
     * - XZ_STAR: isXZ=1, alpha=2, beta=2, tspEncoding=0
     * - XZ_LOC_S: isXZ=1（且非 XZ_STAR）
     * - LOC_S: 其余情况（isXZ=0 且非 LETI）
     */
    public SpatialIndexKind getSpatialIndexKind() {
        if (isLETI()) {
            return SpatialIndexKind.LETI_LOC_S;
        }
        if (isXZStar()) {
            return SpatialIndexKind.XZ_STAR;
        }
        if (isXZ()) {
            return SpatialIndexKind.XZ_LOC_S;
        }
        return SpatialIndexKind.LOC_S;
    }
}
