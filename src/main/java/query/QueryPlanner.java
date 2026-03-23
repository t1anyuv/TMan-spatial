package query;

import config.TableConfig;
import constans.IndexEnum;
// import filter.OIDFilter;  // TODO: 缺失的类
import filter.SpatialFilter;
// import filter.SpatioTemporalFilter;  // TODO: 缺失的类
// import filter.TemporalFilter;  // TODO: 缺失的类
// ByteArrays 是 Scala 对象，通过 Java 包装类调用
import lombok.Getter;
import utils.ByteArraysWrapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
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

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static constans.IndexEnum.INDEX_TYPE.*;

/**
 * 查询规划器
 * <p>
 * 负责执行空间轨迹查询的优化和执行，包括：
 * - RBO（基于规则的优化）：根据主索引类型选择主过滤器
 * - 索引范围查询：从 Redis 或 HBase 获取索引范围
 * - 结果扫描：使用 HBase Scan 或 Get 操作获取查询结果
 * <p>
 * 查询流程：
 * 1. RBO：确定主过滤器和次过滤器
 * 2. 获取索引范围：根据主过滤器获取 RowKey 范围
 * 3. 执行查询：使用 MultiRowRangeFilter 和过滤器列表执行 HBase 查询
 * <p>
 * 注意：当前版本仅支持空间查询。
 *
 */
public class QueryPlanner implements Closeable {
    /** HBase 管理员对象 */
    protected final transient Admin admin;
    /** HBase 连接对象 */
    protected final transient Connection connection;
    /** 过滤器列表 */
    protected List<FilterBase> filters;
    /** 表名 */
    protected String tableName;
    /** 表配置 */
    protected TableConfig tableConfig;
    /** HBase 表对象 */
    protected Table hTable;
    /** 主过滤器（用于主索引查询） */
    protected Filter primaryFilter;
    /** 次过滤器列表（用于次索引查询或后过滤） */
    protected List<Filter> secondaryFilter = new ArrayList<>();
    /**
     * 上一次查询访问的单元格数（VC, Visited Cells）。
     * 基于逻辑索引范围（ranges）统计：contained=true 记 1，否则按 range 宽度累计。
     */
    @Getter
    protected int lastVisitedCells = 0;
    /**
     * 上一次查询用于 HBase {@link MultiRowRangeFilter} 的行键区间条数（与逻辑 {@link IndexRange} 条数不同，次索引展开后通常更大）。
     */
    @Getter
    protected int lastRowRangeCount = 0;

    /**
     * 构造函数
     * 
     * @param filters 过滤器列表
     * @param tableConfig 表配置
     * @param tableName 表名
     * @throws IOException HBase 连接异常
     */
    public QueryPlanner(List<FilterBase> filters, TableConfig tableConfig, String tableName) throws IOException {
        this.filters = filters;
        this.tableConfig = tableConfig;
        this.tableName = tableName;
        Configuration conf = HBaseConfiguration.create();
        this.connection = ConnectionFactory.createConnection(conf);
        this.admin = connection.getAdmin();
        this.hTable = this.connection.getTable(TableName.valueOf(tableName));
    }

    /**
     * 使用新的过滤器列表执行查询
     * 
     * @param filters 新的过滤器列表
     * @param tableConfig 表配置
     * @param tableName 表名
     * @return Tuple2<索引范围数量, 结果扫描器>
     * @throws IOException HBase 操作异常
     */
    public Tuple2<Integer, ResultScanner> executeByFilter(List<FilterBase> filters, TableConfig tableConfig, String tableName) throws IOException {
        reset();
        this.filters = filters;
        this.tableConfig = tableConfig;
        this.tableName = tableName;
        this.hTable = this.connection.getTable(TableName.valueOf(tableName));
        return execute();
    }

    /**
     * 统计并记录 ResultScanner 中的结果数量
     *
     * @param resultScanner 已过滤的结果扫描器
     * @return 结果数量
     */
    public static int getScannerCount(ResultScanner resultScanner) {
        int size = 0;
        if (resultScanner != null) {
            for (Result ignored : resultScanner) {
                size++;
            }
        }
        return size;
    }

    /**
     * 基于成本的优化（Cost-Based Optimization）
     * <p>
     * 注意：当前版本仅支持空间查询，CBO 已简化。
     * 如果需要多种查询类型的成本比较，请扩展此方法。
     */
    public void CBO() {
        // 当前版本仅支持空间查询，无需 CBO
        // 如果未来需要支持多种查询类型，可在此处添加成本比较逻辑
    }

    /**
     * 获取空间过滤器的成本
     * 
     * @param filters 过滤器列表
     * @return Tuple2<空间过滤器, 成本值>
     */
    protected Tuple2<FilterBase, Long> getSpatialCost(List<FilterBase> filters) {
        for (FilterBase filter : filters) {
            if (filter instanceof SpatialFilter) {
                return new Tuple2<>(filter, ((SpatialFilter) filter).getCost(tableName, tableConfig));
            }
        }
        return new Tuple2<>(null, 0L);
    }

    /**
     * 判断是否需要执行 CBO
     * <p>
     * 当前版本仅支持空间查询，始终返回 false
     * 
     * @return false（当前版本不需要 CBO）
     */
    protected boolean needCBO() {
        // 当前版本仅支持空间查询，无需 CBO
        return false;
    }

    /**
     * 执行查询，返回逻辑索引范围数量和结果扫描器
     *
     * @return Tuple2<Integer, ResultScanner> 第一个元素是逻辑索引范围数量（ranges.size()），第二个是结果扫描器
     */
    public Tuple2<Integer, ResultScanner> execute() {
        RBO();
        CBO();
        IndexRangeResult indexResult = getKeyRangesWithIndexCount();
        List<MultiRowRangeFilter.RowRange> rowRanges = indexResult.rowRanges;

        List<Filter> finalFilters = new ArrayList<>();
        if (rowRanges.isEmpty()) {
            System.out.println("[QueryPlanner] No row ranges found, returning empty result.");
            this.lastRowRangeCount = 0;
            return new Tuple2<>(0, null);
        }

        this.lastRowRangeCount = rowRanges.size();

        finalFilters.add(new MultiRowRangeFilter(rowRanges));
        finalFilters.addAll(filters);
        FilterList filterList = new FilterList(finalFilters);
        Scan scan = new Scan();
        scan.setCaching(1000);
        scan.setFilter(filterList);
        ResultScanner returnScanner = null;
        int size = 0;
        try {
            returnScanner = hTable.getScanner(scan);
        } catch (IOException e) {
            System.err.println("[QueryPlanner] Error executing query: " + e.getMessage());
        }
        // 保存访问的单元格数到成员变量
        this.lastVisitedCells = indexResult.visitedCells;
        int indexRangeCount = indexResult.ranges.size();
        return new Tuple2<>(indexRangeCount, returnScanner);
    }

    /**
     * 使用 Get 操作执行查询
     * <p>
     * 与 execute() 的区别：
     * - execute() 使用 Scan 操作，适合范围查询
     * - executeGet() 使用 Get 操作，适合精确查询
     * 
     * @return Result 数组
     */
    public Result[] executeGet() {
        RBO();
        CBO();
        List<MultiRowRangeFilter.RowRange> rowRanges;
        rowRanges = getKeyRanges();
        List<Get> gets = new ArrayList<>();
        for (MultiRowRangeFilter.RowRange rowRange : rowRanges) {
            gets.add(new Get(rowRange.getStartRow()));
//            System.out.println(Arrays.toString(rowRange.getStartRow()));
//            System.out.println(NumberUtil.bytesToHex(rowRange.getStartRow()));
        }

        Result[] resultScanner = null;
        int size = 0;
        int values = 0;
        try {
            resultScanner = hTable.get(gets);
            for (Result result : resultScanner) {
                try {
                    if (!result.isEmpty()) {
                        values++;
                    }
                    boolean filtered = isFiltered(result, filters);
                    if (!filtered) {
                        size++;
                    }

                } catch (Exception e) {
//                    System.out.println(Arrays.toString(result.getRow()));
                }
            }
        } catch (IOException e) {
            System.err.println("[QueryPlanner] Error executing get: " + e.getMessage());
        }
        System.out.println("[QueryPlanner] ExecuteGet - Filtered results: " + size + ", Total values: " + values);
        return resultScanner;
    }

    private static boolean isFiltered(Result result, List<FilterBase> filters) throws IOException {
        for (Cell cell : result.rawCells()) {
            for (FilterBase filter : filters) {
                filter.filterCell(cell);
                if (filter.filterRow()) {
                    break;
                }
            }
        }
        boolean filtered = false;
        for (FilterBase filter : filters) {
            if (filter.filterRow()) {
                filter.reset();
                filtered = true;
                break;
            }
            filter.reset();
        }
        return filtered;
    }

    /**
     * 获取 RowKey 范围列表
     * <p>
     * 根据主过滤器类型，从主索引或次索引获取 RowKey 范围。
     * 
     * @return RowKey 范围列表
     */
    public List<MultiRowRangeFilter.RowRange> getKeyRanges() {
        List<MultiRowRangeFilter.RowRange> ranges;
        if (isPrimaryFilter(primaryFilter)) {
            ranges = getKeysByPrimaryIndex(primaryFilter);
        } else {
            ranges = getKeysBySecondaryIndex(primaryFilter);
        }
        return ranges;
    }

    /**
     * 获取键范围及其逻辑索引范围数量
     *
     * @return IndexRangeResult 包含逻辑索引范围和 HBase 行键范围
     */
    protected IndexRangeResult getKeyRangesWithIndexCount() {
        if (isPrimaryFilter(primaryFilter)) {
            return getKeysByPrimaryIndexCore(primaryFilter);
        } else {
            return getKeysBySecondaryIndexCore(primaryFilter);
        }
    }

    /**
     * 从次索引获取键范围（核心逻辑）
     * <p>
     * 次索引存储在 Redis 中，通过 ZSET 查询获取 RowKey。
     * 当前版本仅支持空间索引。
     * 
     * @param secondaryFilter 次过滤器
     * @return IndexRangeResult 包含逻辑索引范围和 RowKey 范围
     */
    protected IndexRangeResult getKeysBySecondaryIndexCore(Filter secondaryFilter) {
        // 断言：当前版本仅支持空间查询
        assert secondaryFilter instanceof SpatialFilter : "Only SpatialFilter is supported";
        
        List<IndexRange> ranges;
        List<MultiRowRangeFilter.RowRange> rowRanges = new ArrayList<>();
        
        SpatialFilter sFilter = (SpatialFilter) secondaryFilter;
        TableConfig.SpatialIndexKind kind = tableConfig.getSpatialIndexKind();
        switch (kind) {
            case XZ_LOC_S:
            case XZ_STAR:
                ranges = sFilter.getXZRanges(tableName, tableConfig);
                break;
            case LETI_LOC_S:
            case LOC_S:
            default:
                ranges = sFilter.getRanges(tableName, tableConfig);
                break;
        }

        int visitedCells = calculateVisitedCells(ranges);
        secondaryRangesToRowkeys(tableName + "_" + SPATIAL.getIndexName(), ranges, rowRanges);

        return new IndexRangeResult(ranges, rowRanges, visitedCells);
    }

    /**
     * 从次索引获取键范围（包含性能统计）
     * 
     * @param secondaryFilter 次过滤器
     * @return RowKey 范围列表
     */
    protected List<MultiRowRangeFilter.RowRange> getKeysBySecondaryIndex(Filter secondaryFilter) {
        long startTime = System.currentTimeMillis();
        IndexRangeResult result = getKeysBySecondaryIndexCore(secondaryFilter);
        long elapsedTime = System.currentTimeMillis() - startTime;
        int indexRangeCount = result.ranges.size();
        System.out.println("[QueryPlanner] Secondary index query time: " + elapsedTime + "ms, Logical index ranges: " + indexRangeCount + ", Row range count: " + result.rowRanges.size());
        return result.rowRanges;
    }

    /**
     * 将次索引范围转换为 HBase 行键范围
     *
     * @param indexName 索引名称
     * @param ranges    索引范围列表
     * @param rowRanges 输出的行键范围列表
     */
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
            System.err.println("[QueryPlanner] Error converting secondary ranges to rowkeys: " + e.getMessage());
        }
    }

    /**
     * 从主索引获取键范围（核心逻辑）
     * <p>
     * 主索引直接存储在 HBase RowKey 中，通过索引范围转换为 RowKey 范围。
     * 当前版本仅支持空间索引。
     * 
     * @param primaryFilter 主过滤器
     * @return IndexRangeResult 包含逻辑索引范围和 RowKey 范围
     */
    protected IndexRangeResult getKeysByPrimaryIndexCore(Filter primaryFilter) {
        // 断言：当前版本仅支持空间查询
        assert primaryFilter instanceof SpatialFilter : "Only SpatialFilter is supported";
        
        List<IndexRange> ranges;
        List<MultiRowRangeFilter.RowRange> rowRanges = new ArrayList<>();
        
        SpatialFilter sFilter = (SpatialFilter) primaryFilter;
        TableConfig.SpatialIndexKind kind = tableConfig.getSpatialIndexKind();
        switch (kind) {
            case XZ_LOC_S:
            case XZ_STAR:
                ranges = sFilter.getXZRanges(tableName, tableConfig);
                break;
            case LETI_LOC_S:
            case LOC_S:
            default:
                ranges = sFilter.getRanges(tableName, tableConfig);
                break;
        }
        
        int visitedCells = calculateVisitedCells(ranges);

        rangesToRowkey(ranges, rowRanges);

        return new IndexRangeResult(ranges, rowRanges, visitedCells);
    }

    /**
     * 统计访问的单元格数（VC, Visited Cells）。
     * <p>
     * 规则：
     * - 覆盖情况（contained=true）：每个 range 计 1；
     * - 非覆盖情况（contained=false）：按 [lower, upper] 宽度累计。
     *
     * @param ranges 逻辑索引范围列表
     * @return 访问的单元格数
     */
    protected int countVisitedCells(List<IndexRange> ranges) {
        return calculateVisitedCells(ranges);
    }

    private int calculateVisitedCells(List<IndexRange> ranges) {
        if (ranges == null || ranges.isEmpty()) {
            return 0;
        }
        int visitedCells = 0;
        for (IndexRange range : ranges) {
            if (range.contained()) {
                visitedCells += 1;
            } else {
                long width = range.upper() - range.lower() + 1;
                if (width > Integer.MAX_VALUE - visitedCells) {
                    visitedCells = Integer.MAX_VALUE;
                } else {
                    visitedCells += (int) width;
                }
            }
        }
        return visitedCells;
    }

    /**
     * 从主索引获取键范围（包含性能统计）
     * 
     * @param primaryFilter 主过滤器
     * @return RowKey 范围列表
     */
    protected List<MultiRowRangeFilter.RowRange> getKeysByPrimaryIndex(Filter primaryFilter) {
        long startTime = System.currentTimeMillis();
        IndexRangeResult result = getKeysByPrimaryIndexCore(primaryFilter);
        long elapsedTime = System.currentTimeMillis() - startTime;
        int indexRangeCount = result.ranges.size();
        System.out.println("[QueryPlanner] Primary index query time: " + elapsedTime + "ms, Logical index ranges: " + indexRangeCount + ", Row range count: " + result.rowRanges.size());
        return result.rowRanges;
    }

    /**
     * 基于规则的优化（Rule-Based Optimization）
     * <p>
     * 当前版本仅支持空间查询，简化为直接选择空间过滤器作为主过滤器。
     * 规则：选择与表主索引类型匹配的空间过滤器作为主过滤器。
     */
    public void RBO() {
        // 断言：表的主索引必须是空间索引
        assert tableConfig.getPrimary().equals(IndexEnum.INDEX_TYPE.SPATIAL) : 
            "Only SPATIAL primary index is supported";
        
        for (Filter filter : filters) {
            if (isPrimaryFilter(filter)) {
                primaryFilter = filter;
            } else {
                secondaryFilter.add(filter);
            }
        }
        
        if (null == primaryFilter && !filters.isEmpty()) {
            primaryFilter = filters.get(0);
        }
    }

    /**
     * 将索引范围转换为 RowKey 范围
     * <p>
     * 如果配置了分片（shards），则为每个分片生成 RowKey 范围。
     * RowKey 格式（无分片）：[index(8字节)]
     * RowKey 格式（有分片）：[shard(1字节)][index(8字节)]
     * 
     * @param ranges 索引范围列表
     * @param rowRanges 输出的 RowKey 范围列表
     */
    protected void rangesToRowkey(List<IndexRange> ranges, List<MultiRowRangeFilter.RowRange> rowRanges) {
        for (IndexRange range : ranges) {
            byte[] startRow = new byte[8];
            byte[] endRow = new byte[8];
            try {
                ByteArraysWrapper.writeLong(range.lower(), startRow, 0);
                ByteArraysWrapper.writeLong(range.upper() + 1L, endRow, 0);
                Short shards = tableConfig.getShards();
                if (null != shards) {
                    for (int i = 0; i < shards; i++) {
                        byte[] startRowF = new byte[9];
                        byte[] endRowF = new byte[9];
                        startRowF[0] = (byte) i;
                        endRowF[0] = (byte) i;
                        ByteArraysWrapper.writeLong(range.lower(), startRowF, 1);
                        ByteArraysWrapper.writeLong(range.upper() + 1L, endRowF, 1);
                        rowRanges.add(new MultiRowRangeFilter.RowRange(startRowF, true, endRowF, true));
                    }
                } else {
                    rowRanges.add(new MultiRowRangeFilter.RowRange(startRow, true, endRow, true));
                }
            } catch (Exception e) {
//                System.out.println();
            }
        }
    }

    /**
     * 将索引范围转换为 RowKey 范围（静态方法）
     * 
     * @param ranges 索引范围列表
     * @param config 表配置
     * @return RowKey 范围列表
     */
    public static List<MultiRowRangeFilter.RowRange> rangesToRowkey(List<IndexRange> ranges, TableConfig config) {
        List<MultiRowRangeFilter.RowRange> rowRanges = new ArrayList<>();
        for (IndexRange range : ranges) {
            byte[] startRow = new byte[8];
            byte[] endRow = new byte[8];
            try {
                ByteArraysWrapper.writeLong(range.lower(), startRow, 0);
                ByteArraysWrapper.writeLong(range.upper() + 1L, endRow, 0);
                Short shards = config.getShards();
                if (null != shards) {
                    for (int i = 0; i < shards; i++) {
                        byte[] startRowF = new byte[9];
                        byte[] endRowF = new byte[9];
                        startRowF[0] = (byte) i;
                        endRowF[0] = (byte) i;
                        ByteArraysWrapper.writeLong(range.lower(), startRowF, 1);
                        ByteArraysWrapper.writeLong(range.upper() + 1L, endRowF, 1);
                        rowRanges.add(new MultiRowRangeFilter.RowRange(startRowF, true, endRowF, true));
                    }
                } else {
                    rowRanges.add(new MultiRowRangeFilter.RowRange(startRow, true, endRow, true));
                }
            } catch (Exception e) {
//                System.out.println();
            }
        }
        return rowRanges;
    }

    /**
     * 判断过滤器是否匹配表的主索引类型
     * <p>
     * 当前版本仅支持空间索引。
     * 
     * @param filter 过滤器
     * @return true 如果过滤器是空间过滤器且表主索引是空间索引，false 否则
     */
    protected boolean isPrimaryFilter(Filter filter) {
        return filter instanceof SpatialFilter && 
               tableConfig.getPrimary().equals(IndexEnum.INDEX_TYPE.SPATIAL);
    }

    /**
     * 内部类：用于返回索引范围查询的结果
     */
    protected static class IndexRangeResult {
        /**
         * 逻辑索引范围列表: 包括索引值的下界和上界
         */
        final List<IndexRange> ranges;
        /**
         * HBase 行键范围列表
         */
        final List<MultiRowRangeFilter.RowRange> rowRanges;
        /**
         * 访问的单元格数（VC）。
         * 由 ranges 直接推导，表示访问成本近似值。
         */
        final int visitedCells;

        IndexRangeResult(List<IndexRange> ranges, List<MultiRowRangeFilter.RowRange> rowRanges, int visitedCells) {
            this.ranges = ranges;
            this.rowRanges = rowRanges;
            this.visitedCells = visitedCells;
        }

        IndexRangeResult(List<IndexRange> ranges, List<MultiRowRangeFilter.RowRange> rowRanges) {
            this(ranges, rowRanges, 0);
        }
    }

    public void reset() {
        this.tableConfig = null;
        this.primaryFilter = null;
        this.filters = null;
        this.tableName = null;
        this.secondaryFilter = new ArrayList<>();
        this.lastVisitedCells = 0;
        this.lastRowRangeCount = 0;
    }

    @Override
    public void close() throws IOException {
        this.admin.close();
        this.connection.close();
    }
}
