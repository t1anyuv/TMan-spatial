package query;

import config.TableConfig;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.*;
import scala.Tuple2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * 计数查询规划器
 * <p>
 * 继承自 QueryPlanner，专门用于执行计数查询。
 * 与 QueryPlanner 的区别：
 * - 使用 FirstKeyOnlyFilter 优化，只读取 RowKey 而不读取数据
 * - 返回的 ResultScanner 仅用于计数，不包含实际数据
 * - 日志输出使用 [CountPlanner] 前缀以区分候选计数阶段
 * <p>
 * 适用场景：
 * - 查询结果数量统计
 * - 查询成本估算
 * - 查询优化决策
 * 
 */
public class CountPlanner extends QueryPlanner {

    /**
     * 构造函数
     * 
     * @param filters 过滤器列表
     * @param tableConfig 表配置
     * @param tableName 表名
     * @throws IOException HBase 连接异常
     */
    public CountPlanner(List<FilterBase> filters, TableConfig tableConfig, String tableName) throws IOException {
        super(filters, tableConfig, tableName);
    }

    /**
     * 执行计数查询
     * <p>
     * 使用 FirstKeyOnlyFilter 优化，只读取 RowKey 而不读取数据列，
     * 从而大幅提升计数查询的性能。
     * 
     * @return Tuple2<逻辑 IndexRange 条数, 结果扫描器> 结果扫描器仅用于计数
     */
    @Override
    public Tuple2<Integer, ResultScanner> execute() {
        RBO();
        CBO();
        QueryPlanner.IndexRangeResult indexResult = getKeyRangesWithIndexCount();
        List<MultiRowRangeFilter.RowRange> rowRanges = indexResult.rowRanges;

        if (rowRanges.isEmpty()) {
            return null;
        }
        
        List<Filter> finalFilters = new ArrayList<>();
        finalFilters.add(new MultiRowRangeFilter(rowRanges));
        finalFilters.add(new FirstKeyOnlyFilter());
        
        FilterList filterList = new FilterList(finalFilters);
        Scan scan = new Scan();
        scan.setCaching(1000);
        scan.setFilter(filterList);
        
        ResultScanner resultScanner = null;
        try {
            resultScanner = hTable.getScanner(scan);
        } catch (IOException e) {
            System.err.println("[CountPlanner] Error executing count query: " + e.getMessage());
        }
        return new Tuple2<>(indexResult.ranges.size(), resultScanner);
    }

    /**
     * 重写父类方法，使用 [CountPlanner] 前缀区分候选计数阶段
     */
    @Override
    protected List<MultiRowRangeFilter.RowRange> getKeysByPrimaryIndex(Filter primaryFilter) {
        long time = System.currentTimeMillis();
        IndexRangeResult result = getKeysByPrimaryIndexCore(primaryFilter);
        System.out.println("[CountPlanner] compute row key time: " + (System.currentTimeMillis() - time));
        int rangeSize = result.ranges.size();
        System.out.println("[CountPlanner] logical index range size: " + rangeSize);
        return result.rowRanges;
    }

    /**
     * 重写父类方法，使用 [CountPlanner] 前缀区分候选计数阶段
     */
    @Override
    protected List<MultiRowRangeFilter.RowRange> getKeysBySecondaryIndex(Filter secondaryFilter) {
        long time = System.currentTimeMillis();
        IndexRangeResult result = getKeysBySecondaryIndexCore(secondaryFilter);
        System.out.println("[CountPlanner] index time: " + (System.currentTimeMillis() - time));
        System.out.println("[CountPlanner] logical index range size: " + result.ranges.size() + ", row range count: " + result.rowRanges.size());
        return result.rowRanges;
    }
}
