package experiments.validate;

import com.esri.core.geometry.Envelope;
import config.TableConfig;
import filter.SpatialFilter;

import java.io.IOException;

/**
 * LETI 专用正确性验证入口，使用普通空间过滤器执行查询。
 */
public class LetiSpatialQueryValidate extends SpatialQueryValidate {

    public static void main(String[] args) throws IOException {
        runValidate(args, new LetiSpatialQueryValidate());
    }

    @Override
    protected SpatialFilter createSpatialFilter(Envelope env, TableConfig tableConfig) {
        return new SpatialFilter(
                com.esri.core.geometry.GeometryEngine.geometryToWkt(env, 0),
                tableConfig.getCompressType().toString());
    }
}
