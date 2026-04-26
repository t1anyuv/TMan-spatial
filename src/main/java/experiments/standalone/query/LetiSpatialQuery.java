package experiments.standalone.query;

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

}
