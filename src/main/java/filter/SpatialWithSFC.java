package filter;

import client.Constants;
import com.esri.core.geometry.*;
import config.TableConfig;
import index.LMSFCIndex;
import index.BMTreeIndex;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.locationtech.sfcurve.IndexRange;
import scala.Tuple2;

import java.util.List;

public class SpatialWithSFC extends SpatialFilter {
    private boolean foundMaxSFC = false;
    private long maxSFC;
    private long queryMinSFC;

    public SpatialWithSFC(String geom, String operationType, String compressType, 
                          long queryMinSFC) {
        super(geom, operationType, compressType);
        this.queryMinSFC = queryMinSFC;
    }

    public SpatialWithSFC(String geom, String compressType,
                              long queryMinSFC) {
        super(geom, compressType);
        this.queryMinSFC = queryMinSFC;
    }

    @Override
    protected ReturnCode filterCellBefore(Cell c) {
        String qualifier = Bytes.toString(CellUtil.cloneQualifier(c));

        if (!this.filterRow && qualifier.equals(Constants.MAX_SFC)) {
            foundMaxSFC = true;
            maxSFC = Bytes.toLong(CellUtil.cloneValue(c));
            if (maxSFC < queryMinSFC) {
                this.filterRow = true;
                return ReturnCode.NEXT_ROW;
            }
        }

        return null; 
    }

    /**
     * 获取 LMSFC 索引的范围
     * 
     * @param tableName 表名
     * @param config 表配置
     * @return 索引范围列表
     */
    public List<IndexRange> getLMSFCRanges(String tableName, TableConfig config) {
        Envelope2D envelope = new Envelope2D();
        this.geometry.queryEnvelope2D(envelope);

        LMSFCIndex lmsfcIndex;
        if (config.getEnvelope() != null) {
            lmsfcIndex = LMSFCIndex.apply(
                    (short) config.getResolution(),
                    new Tuple2<>(config.getEnvelope().getXMin(), config.getEnvelope().getXMax()),
                    new Tuple2<>(config.getEnvelope().getYMin(), config.getEnvelope().getYMax()),
                    config.getThetaConfig());
        } else {
            lmsfcIndex = LMSFCIndex.apply(
                    (short) config.getResolution(),
                    new Tuple2<>(-180.0, 180.0),
                    new Tuple2<>(-90.0, 90.0),
                    config.getThetaConfig());
        }

        return lmsfcIndex.rangesWithSplit(envelope.xmin, envelope.ymin, envelope.xmax, envelope.ymax, 3);
    }

    /**
     * 获取 BMTree 索引的范围
     * 
     * @param tableName 表名
     * @param config 表配置
     * @return 索引范围列表
     */
    public List<IndexRange> getBMTreeRanges(String tableName, TableConfig config) {
        Envelope2D envelope = new Envelope2D();
        this.geometry.queryEnvelope2D(envelope);

        BMTreeIndex bmtreeIndex;
        if (config.getEnvelope() != null) {
            bmtreeIndex = BMTreeIndex.apply(
                    (short) config.getResolution(),
                    new Tuple2<>(config.getEnvelope().getXMin(), config.getEnvelope().getXMax()),
                    new Tuple2<>(config.getEnvelope().getYMin(), config.getEnvelope().getYMax()),
                    config.getBMTreeConfigPath(),
                    config.getBMTreeBitLength());
        } else {
            bmtreeIndex = BMTreeIndex.apply(
                    (short) config.getResolution(),
                    new Tuple2<>(-180.0, 180.0),
                    new Tuple2<>(-90.0, 90.0),
                    config.getBMTreeConfigPath(),
                    config.getBMTreeBitLength());
        }

        return bmtreeIndex.ranges(envelope.xmin, envelope.ymin, envelope.xmax, envelope.ymax);
    }
}