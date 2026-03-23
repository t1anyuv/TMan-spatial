package query;

import config.TableConfig;
import constans.IndexEnum;
import filter.SpatialFilter;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.MultiRowRangeFilter;
import org.locationtech.sfcurve.IndexRange;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;
import redis.clients.jedis.resps.Tuple;
import scala.Tuple2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static constans.IndexEnum.INDEX_TYPE.*;

/**
 * XZ Ordering 查询规划器
 * <p>
 * 继承自 QueryPlanner，专门用于 XZ Ordering 索引的查询。
 * 当前版本仅支持空间查询。
 * 
 * @author hehuajun
 * @date 2023/2/12 20:26
 */
public class XZOrderingQueryPlanner extends QueryPlanner {

    public XZOrderingQueryPlanner(List<FilterBase> filters, TableConfig tableConfig, String tableName) throws IOException {
        super(filters, tableConfig, tableName);
    }

    /**
     * 执行查询，返回逻辑索引范围数量和结果数量
     * 注意：此方法返回 Tuple2<Integer, Integer> 而不是 Tuple2<Integer, ResultScanner>
     * 由于 Java 不支持泛型协变返回类型，此方法不能重写基类的 execute() 方法
     */
    public Tuple2<Integer, Integer> executeByXZ() {
        RBO();
        IndexRangeResult indexResult = getKeyRangesWithIndexCount();
        List<MultiRowRangeFilter.RowRange> rowRanges = indexResult.rowRanges;
        
        if (rowRanges.isEmpty()) {
            return new Tuple2<>(0, 0);
        }
        
        List<Filter> finalFilters = new ArrayList<>();
        finalFilters.add(new MultiRowRangeFilter(rowRanges));
        finalFilters.addAll(filters);
        
        FilterList filterList = new FilterList(finalFilters);
        Scan scan = new Scan();
        scan.setCaching(1000);
        scan.setFilter(filterList);
        
        int size = 0;
        try {
            ResultScanner resultScanner = hTable.getScanner(scan);
            size = getScannerCount(resultScanner);
            resultScanner.close();
        } catch (IOException e) {
            System.err.println("[XZOrderingQueryPlanner] Error executing query: " + e.getMessage());
        }
        
        int indexRangeCount = indexResult.ranges.size();
        return new Tuple2<>(indexRangeCount, size);
    }

    @Override
    public Result[] executeGet() {
        RBO();
        List<MultiRowRangeFilter.RowRange> rowRanges = getKeyRanges();
        List<Get> gets = new ArrayList<>();
        for (MultiRowRangeFilter.RowRange rowRange : rowRanges) {
            gets.add(new Get(rowRange.getStartRow()));
        }

        Result[] results = null;
        try {
            results = hTable.get(gets);
        } catch (IOException e) {
            System.err.println("[XZOrderingQueryPlanner] Error executing get: " + e.getMessage());
        }
        return results;
    }

    /**
     * 使用新的过滤器列表执行查询
     * 
     * @param filters 新的过滤器列表
     * @param tableConfig 表配置
     * @param tableName 表名
     * @return Tuple2<索引范围数量, 结果数量>
     * @throws IOException HBase 操作异常
     */
    public Tuple2<Integer, Integer> executeByFilterXZ(List<FilterBase> filters, TableConfig tableConfig, String tableName) throws IOException {
        reset();
        this.filters = filters;
        this.tableConfig = tableConfig;
        this.tableName = tableName;
        this.hTable = this.connection.getTable(TableName.valueOf(tableName));
        return executeByXZ();
    }

    @Override
    protected IndexRangeResult getKeysBySecondaryIndexCore(Filter secondaryFilter) {
        // 断言：当前版本仅支持空间查询
        assert secondaryFilter instanceof SpatialFilter : "Only SpatialFilter is supported";
        
        List<IndexRange> ranges;
        List<MultiRowRangeFilter.RowRange> rowRanges = new ArrayList<>();
        
        SpatialFilter sFilter = (SpatialFilter) secondaryFilter;
        ranges = sFilter.getXZRanges(tableName, tableConfig);
        secondaryRangesToRowkeys(tableName + "_" + SPATIAL.getIndexName(), ranges, rowRanges);
        
        return new IndexRangeResult(ranges, rowRanges);
    }

    @Override
    protected List<MultiRowRangeFilter.RowRange> getKeysBySecondaryIndex(Filter secondaryFilter) {
        long startTime = System.currentTimeMillis();
        IndexRangeResult result = getKeysBySecondaryIndexCore(secondaryFilter);
        long elapsedTime = System.currentTimeMillis() - startTime;
        int indexRangeCount = result.ranges.size();
        System.out.println("[XZOrderingQueryPlanner] Secondary index query time: " + elapsedTime + "ms, Logical index ranges: " + indexRangeCount + ", Row range count: " + result.rowRanges.size());
        return result.rowRanges;
    }

    @Override
    protected IndexRangeResult getKeysByPrimaryIndexCore(Filter primaryFilter) {
        // 断言：当前版本仅支持空间查询
        assert primaryFilter instanceof SpatialFilter : "Only SpatialFilter is supported";
        
        List<IndexRange> ranges;
        List<MultiRowRangeFilter.RowRange> rowRanges = new ArrayList<>();
        
        SpatialFilter sFilter = (SpatialFilter) primaryFilter;
        ranges = sFilter.getXZRanges(tableName, tableConfig);
        rangesToRowkey(ranges, rowRanges);
        
        return new IndexRangeResult(ranges, rowRanges);
    }

    @Override
    protected List<MultiRowRangeFilter.RowRange> getKeysByPrimaryIndex(Filter primaryFilter) {
        long startTime = System.currentTimeMillis();
        IndexRangeResult result = getKeysByPrimaryIndexCore(primaryFilter);
        long elapsedTime = System.currentTimeMillis() - startTime;
        int indexRangeCount = result.ranges.size();
        System.out.println("[XZOrderingQueryPlanner] Primary index query time: " + elapsedTime + "ms, Logical index ranges: " + indexRangeCount + ", Row range count: " + result.rowRanges.size());
        return result.rowRanges;
    }

    @Override
    protected void secondaryRangesToRowkeys(String indexName, List<IndexRange> ranges, List<MultiRowRangeFilter.RowRange> rowRanges) {
        try (Jedis jedis = new Jedis(tableConfig.getRedisHost(), 6379)) {
            Pipeline pipeline = jedis.pipelined();
            List<Response<List<Tuple>>> responses = new ArrayList<>();
            for (IndexRange range : ranges) {
                responses.add(pipeline.zrangeByScoreWithScores(indexName, range.lower(), range.upper()));
            }
            pipeline.sync();
            for (Response<List<Tuple>> response : responses) {
                for (Tuple tuple : response.get()) {
                    rowRanges.add(new MultiRowRangeFilter.RowRange(tuple.getBinaryElement(), true, tuple.getBinaryElement(), true));
                }
            }
        } catch (Exception e) {
            System.err.println("[XZOrderingQueryPlanner] Error converting secondary ranges to rowkeys: " + e.getMessage());
        }
    }

    @Override
    protected boolean isPrimaryFilter(Filter filter) {
        return filter instanceof SpatialFilter && 
               tableConfig.getPrimary().equals(IndexEnum.INDEX_TYPE.SPATIAL);
    }
}
