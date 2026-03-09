package config;

import com.esri.core.geometry.Envelope;
import constans.IndexEnum;
// import index.TimePeriod;  // TODO: 缺失的类，需要实现或移除相关功能
import preprocess.compress.IIntegerCompress;
import lombok.Getter;
import lombok.Setter;
import scala.Enumeration;

import java.io.Serializable;


@Getter
@Setter
public class TableConfig implements Serializable {
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
     * RL 编码标志
     *
     * @return boolean
     */
    public boolean isRLEncoding() {
        return rlEncoding == 1;
    }

    /**
     * 是否使用自适应划分
     *
     * @return boolean
     */
    public boolean isAdaptivePartition() {
        return adaptivePartition == 1;
    }
}
