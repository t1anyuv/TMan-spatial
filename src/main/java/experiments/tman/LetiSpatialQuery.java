package experiments.tman;

import com.esri.core.geometry.Envelope;
import com.esri.core.geometry.GeometryEngine;
import config.TableConfig;
import filter.SpatialFilter;
import org.apache.hadoop.hbase.filter.FilterBase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class LetiSpatialQuery extends SpatialQuery {
    public static void main(String[] args) throws IOException {
        LetiSpatialQuery stQuery = new LetiSpatialQuery();
        stQuery.executeQuery(args);
    }

    @Override
    public List<FilterBase> getFilters(String condition, TableConfig tableConfig) {
        String[] xy = condition.trim().split(",");
        Envelope env = new Envelope(Double.parseDouble(xy[0].trim()), Double.parseDouble(xy[1].trim()), Double.parseDouble(xy[2].trim()), Double.parseDouble(xy[3].trim()));
        SpatialFilter spatialFilter = new SpatialFilter(
                GeometryEngine.geometryToWkt(env, 0),
                tableConfig.getCompressType().toString()
        );
        List<FilterBase> filters = new ArrayList<>();
        filters.add(spatialFilter);
        return filters;
    }
}
