package filter;

import client.Constants;
import com.esri.core.geometry.Envelope2D;
import config.TableConfig;
import index.BMTreeIndex;
import index.LMSFCIndex;
import lombok.Getter;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.locationtech.sfcurve.IndexRange;
import scala.Tuple2;

import java.util.List;

@Getter
public class SpatialWithSFC extends SpatialFilter {
    private final long queryMinSFC;

    public SpatialWithSFC(String geom, String operationType, String compressType, long queryMinSFC) {
        super(geom, operationType, compressType);
        this.queryMinSFC = queryMinSFC;
    }

    public SpatialWithSFC(String geom, String compressType, long queryMinSFC) {
        super(geom, compressType);
        this.queryMinSFC = queryMinSFC;
    }

    public Filter createMaxSFCFilter() {
        SingleColumnValueFilter filter = new SingleColumnValueFilter(
                Bytes.toBytes(Constants.DEFAULT_CF),
                Bytes.toBytes(Constants.MAX_SFC),
                CompareOperator.GREATER_OR_EQUAL,
                Bytes.toBytes(queryMinSFC)
        );
        filter.setFilterIfMissing(true);
        filter.setLatestVersionOnly(true);
        return filter;
    }

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

        return lmsfcIndex.ranges(envelope.xmin, envelope.ymin, envelope.xmax, envelope.ymax);
    }

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
