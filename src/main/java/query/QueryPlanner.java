package query;

import config.TableConfig;
import constans.IndexEnum;
import filter.SpatialFilter;
import filter.SpatialWithSFC;
import lombok.Getter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.MultiRowRangeFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.locationtech.sfcurve.IndexRange;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;
import redis.clients.jedis.resps.Tuple;
import scala.Tuple2;
import utils.ByteArraysWrapper;
import utils.RedisPoolManager;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static client.Constants.DEFAULT_CF;
import static client.Constants.GEOM_X;
import static client.Constants.GEOM_Y;
import static client.Constants.O_ID;
import static client.Constants.T_ID;
import static constans.IndexEnum.INDEX_TYPE.SPATIAL;
import static org.apache.hadoop.hbase.util.Bytes.toBytes;

/**
 * Builds row-key ranges from spatial filters and runs the final HBase scan.
 */
public class QueryPlanner implements Closeable {
    protected final transient Admin admin;
    protected final transient Connection connection;

    protected List<FilterBase> filters;
    protected String tableName;
    protected TableConfig tableConfig;
    protected Table hTable;
    protected Filter primaryFilter;
    protected List<Filter> secondaryFilter = new ArrayList<>();

    @Getter
    protected int lastRowRangeCount = 0;

    @Getter
    protected List<MultiRowRangeFilter.RowRange> lastComputedRowRanges = new ArrayList<>();

    public QueryPlanner(List<FilterBase> filters, TableConfig tableConfig, String tableName) throws IOException {
        this.filters = filters;
        this.tableConfig = tableConfig;
        this.tableName = tableName;
        Configuration conf = HBaseConfiguration.create();
        this.connection = ConnectionFactory.createConnection(conf);
        this.admin = connection.getAdmin();
        this.hTable = this.connection.getTable(TableName.valueOf(tableName));
    }

    public Tuple2<Integer, ResultScanner> executeByFilter(List<FilterBase> filters, TableConfig tableConfig, String tableName) throws IOException {
        reset();
        this.filters = filters;
        this.tableConfig = tableConfig;
        this.tableName = tableName;
        this.hTable = this.connection.getTable(TableName.valueOf(tableName));
        return execute();
    }

    public static int getScannerCount(ResultScanner resultScanner) {
        int size = 0;
        if (resultScanner != null) {
            for (Result ignored : resultScanner) {
                size++;
            }
        }
        return size;
    }

    public Tuple2<Integer, ResultScanner> execute() {
        selectPrimaryFilter();
        IndexRangeResult indexResult = getKeyRangesWithIndexCount();
        List<MultiRowRangeFilter.RowRange> rowRanges = indexResult.rowRanges;

        if (rowRanges.isEmpty()) {
            System.out.println("[QueryPlanner] No row ranges found, returning empty result.");
            lastRowRangeCount = 0;
            lastComputedRowRanges = new ArrayList<>();
            return new Tuple2<>(0, null);
        }

        lastRowRangeCount = rowRanges.size();
        lastComputedRowRanges = new ArrayList<>(rowRanges);

        Scan scan = buildScan(rowRanges, true);
        ResultScanner resultScanner = null;
        try {
            resultScanner = hTable.getScanner(scan);
        } catch (IOException e) {
            System.err.println("[QueryPlanner] Error executing query: " + e.getMessage());
        }
        return new Tuple2<>(indexResult.ranges.size(), resultScanner);
    }

    protected IndexRangeResult getKeyRangesWithIndexCount() {
        if (isPrimaryFilter(primaryFilter)) {
            return getKeysByPrimaryIndexCore(primaryFilter);
        }
        return getKeysBySecondaryIndexCore(primaryFilter);
    }

    protected IndexRangeResult getKeysBySecondaryIndexCore(Filter secondaryFilter) {
        assert secondaryFilter instanceof SpatialFilter : "Only SpatialFilter is supported";

        SpatialFilter spatialFilter = (SpatialFilter) secondaryFilter;
        List<IndexRange> ranges = getSecondaryIndexRanges(spatialFilter);
        List<MultiRowRangeFilter.RowRange> rowRanges = new ArrayList<>();
        secondaryRangesToRowkeys(tableName + "_" + SPATIAL.getIndexName(), ranges, rowRanges);
        return new IndexRangeResult(ranges, rowRanges);
    }

    protected void secondaryRangesToRowkeys(String indexName, List<IndexRange> ranges, List<MultiRowRangeFilter.RowRange> rowRanges) {
        try (Jedis jedis = RedisPoolManager.getResource(tableConfig.getRedisHost())) {
            Pipeline pipeline = jedis.pipelined();
            List<Response<List<Tuple>>> responses = new ArrayList<>(ranges.size());
            for (IndexRange range : ranges) {
                responses.add(pipeline.zrangeByScoreWithScores(indexName, range.lower(), range.upper()));
            }
            pipeline.sync();

            for (Response<List<Tuple>> response : responses) {
                for (Tuple tuple : response.get()) {
                    byte[] rowKey = tuple.getBinaryElement();
                    rowRanges.add(new MultiRowRangeFilter.RowRange(rowKey, true, rowKey, true));
                }
            }
        } catch (Exception e) {
            System.err.println("[QueryPlanner] Error converting secondary ranges to rowkeys: " + e.getMessage());
        }
    }

    protected IndexRangeResult getKeysByPrimaryIndexCore(Filter primaryFilter) {
        assert primaryFilter instanceof SpatialFilter : "Only SpatialFilter is supported";

        SpatialFilter spatialFilter = (SpatialFilter) primaryFilter;
        List<IndexRange> ranges = getPrimaryIndexRanges(spatialFilter);
        List<MultiRowRangeFilter.RowRange> rowRanges = new ArrayList<>();
        rangesToRowkey(ranges, rowRanges);
        return new IndexRangeResult(ranges, rowRanges);
    }

    protected void rangesToRowkey(List<IndexRange> ranges, List<MultiRowRangeFilter.RowRange> rowRanges) {
        Short shards = tableConfig.getShards();
        for (IndexRange range : ranges) {
            appendRowRange(rowRanges, range.lower(), range.upper(), shards);
        }
    }

    public static List<MultiRowRangeFilter.RowRange> rangesToRowkey(List<IndexRange> ranges, TableConfig config) {
        List<MultiRowRangeFilter.RowRange> rowRanges = new ArrayList<>();
        Short shards = config.getShards();
        for (IndexRange range : ranges) {
            appendRowRange(rowRanges, range.lower(), range.upper(), shards);
        }
        return rowRanges;
    }

    protected boolean isPrimaryFilter(Filter filter) {
        return filter instanceof SpatialFilter && tableConfig.getPrimary().equals(IndexEnum.INDEX_TYPE.SPATIAL);
    }

    public void reset() {
        this.tableConfig = null;
        this.primaryFilter = null;
        this.filters = null;
        this.tableName = null;
        this.secondaryFilter = new ArrayList<>();
        this.lastRowRangeCount = 0;
        this.lastComputedRowRanges = new ArrayList<>();
    }

    @Override
    public void close() throws IOException {
        this.admin.close();
        this.connection.close();
    }

    protected Scan buildScan(List<MultiRowRangeFilter.RowRange> rowRanges, boolean includeUserFilters) {
        List<Filter> scanFilters = new ArrayList<>();
        scanFilters.add(new MultiRowRangeFilter(rowRanges));
        addEarlyServerSideFilters(scanFilters);
        if (includeUserFilters && filters != null) {
            scanFilters.addAll(filters);
        }

        Scan scan = new Scan();
        scan.setCaching(1000);
        applyScanColumnPruning(scan);
        scan.setFilter(new FilterList(scanFilters));
        return scan;
    }

    protected void addEarlyServerSideFilters(List<Filter> finalFilters) {
        if (filters == null || filters.isEmpty()) {
            return;
        }
        for (FilterBase filter : filters) {
            if (filter instanceof SpatialWithSFC) {
                finalFilters.add(((SpatialWithSFC) filter).createMaxSFCFilter());
            }
        }
    }

    protected List<IndexRange> getPrimaryIndexRanges(SpatialFilter spatialFilter) {
        switch (tableConfig.getSpatialIndexKind()) {
            case LMSFC:
                return requireSFCFilter(spatialFilter, "LMSFC").getLMSFCRanges(tableName, tableConfig);
            case BMTREE:
                return requireSFCFilter(spatialFilter, "BMTree").getBMTreeRanges(tableName, tableConfig);
            case XZPlus:
            case XZ_STAR:
                return spatialFilter.getXZRanges(tableName, tableConfig);
            case LETI:
            case TShape:
            default:
                return spatialFilter.getRanges(tableName, tableConfig);
        }
    }

    protected List<IndexRange> getSecondaryIndexRanges(SpatialFilter spatialFilter) {
        switch (tableConfig.getSpatialIndexKind()) {
            case XZPlus:
            case XZ_STAR:
                return spatialFilter.getXZRanges(tableName, tableConfig);
            case LETI:
            case TShape:
            default:
                return spatialFilter.getRanges(tableName, tableConfig);
        }
    }

    protected void selectPrimaryFilter() {
        assert tableConfig.getPrimary().equals(IndexEnum.INDEX_TYPE.SPATIAL)
                : "Only SPATIAL primary index is supported";

        primaryFilter = null;
        secondaryFilter = new ArrayList<>();
        if (filters == null || filters.isEmpty()) {
            return;
        }

        for (Filter filter : filters) {
            if (primaryFilter == null && isPrimaryFilter(filter)) {
                primaryFilter = filter;
            } else {
                secondaryFilter.add(filter);
            }
        }

        if (primaryFilter == null) {
            primaryFilter = filters.get(0);
        }
    }

    protected void applyScanColumnPruning(Scan scan) {
        if (!canApplyLetiColumnPruning()) {
            return;
        }

        byte[] cf = toBytes(DEFAULT_CF);
        scan.addColumn(cf, toBytes(O_ID));
        scan.addColumn(cf, toBytes(T_ID));
        scan.addColumn(cf, toBytes(GEOM_X));
        scan.addColumn(cf, toBytes(GEOM_Y));
    }

    protected boolean canApplyLetiColumnPruning() {
        if (!(primaryFilter instanceof SpatialFilter)) {
            return false;
        }
        if (tableConfig == null || tableConfig.getSpatialIndexKind() != TableConfig.SpatialIndexKind.LETI) {
            return false;
        }
        if (filters == null || filters.size() != 1) {
            return false;
        }
        return secondaryFilter == null || secondaryFilter.isEmpty();
    }

    private static void appendRowRange(List<MultiRowRangeFilter.RowRange> rowRanges, long lower, long upper, Short shards) {
        try {
            if (shards != null) {
                for (int shard = 0; shard < shards; shard++) {
                    byte[] startRow = new byte[9];
                    byte[] endRow = new byte[9];
                    startRow[0] = (byte) shard;
                    endRow[0] = (byte) shard;
                    ByteArraysWrapper.writeLong(lower, startRow, 1);
                    ByteArraysWrapper.writeLong(upper + 1L, endRow, 1);
                    rowRanges.add(new MultiRowRangeFilter.RowRange(startRow, true, endRow, false));
                }
                return;
            }

            byte[] startRow = new byte[8];
            byte[] endRow = new byte[8];
            ByteArraysWrapper.writeLong(lower, startRow, 0);
            ByteArraysWrapper.writeLong(upper + 1L, endRow, 0);
            rowRanges.add(new MultiRowRangeFilter.RowRange(startRow, true, endRow, false));
        } catch (Exception ignored) {
        }
    }

    private SpatialWithSFC requireSFCFilter(SpatialFilter spatialFilter, String method) {
        if (spatialFilter instanceof SpatialWithSFC) {
            return (SpatialWithSFC) spatialFilter;
        }
        throw new IllegalStateException(method + " index requires SpatialWithSFC filter");
    }

    protected static class IndexRangeResult {
        final List<IndexRange> ranges;
        final List<MultiRowRangeFilter.RowRange> rowRanges;

        IndexRangeResult(List<IndexRange> ranges, List<MultiRowRangeFilter.RowRange> rowRanges) {
            this.ranges = ranges;
            this.rowRanges = rowRanges;
        }
    }
}
