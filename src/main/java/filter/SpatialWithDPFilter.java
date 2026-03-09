package filter;

import com.esri.core.geometry.Geometry;
import com.esri.core.geometry.OperatorIntersects;
import com.esri.core.geometry.SpatialReference;
import constans.IndexEnum;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.util.Bytes;

import static client.Constants.PIVOT_MBR;


public class SpatialWithDPFilter extends SpatialFilter {

    public SpatialWithDPFilter(String geom, String operationType, String compressType) {
        super(geom, operationType, compressType);
    }

    public SpatialWithDPFilter(String geom, String compressType) {
        super(geom, compressType);
    }

    /**
     * 重写 filterCellBefore 方法以添加 DP 检查逻辑
     */
    @Override
    protected ReturnCode filterCellBefore(Cell c) {
        if (!this.filterRow && Bytes.toString(CellUtil.cloneQualifier(c)).equals(PIVOT_MBR) && !checked) {
            Geometry dpFeature = importerWKT.execute(0, Geometry.Type.Unknown, Bytes.toString(CellUtil.cloneValue(c)), null);
            if (!OperatorIntersects.local().execute(geometry, dpFeature, SpatialReference.create(4326), null)) {
                this.filterRow = true;
                return ReturnCode.NEXT_ROW;
            }
        }
        return null; // 继续执行默认逻辑
    }

    public IndexEnum.INDEX_TYPE getIndexType() {
        return indexType;
    }
}
