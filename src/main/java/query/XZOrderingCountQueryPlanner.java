package query;

import config.TableConfig;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.MultiRowRangeFilter;
import scala.Tuple2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class XZOrderingCountQueryPlanner extends XZOrderingQueryPlanner {

    public XZOrderingCountQueryPlanner(List<FilterBase> filters, TableConfig tableConfig, String tableName) throws IOException {
        super(filters, tableConfig, tableName);
    }

    /**
     * 执行查询，返回逻辑索引范围数量和结果数量
     * 重写父类的 executeByXZ() 方法
     */
    @Override
    public Tuple2<Integer, Integer> executeByXZ() {
        RBO();
        List<MultiRowRangeFilter.RowRange> rowRanges = getKeyRanges();
        
        if (rowRanges.isEmpty()) {
            return new Tuple2<>(0, 0);
        }
        
        List<Filter> finalFilters = new ArrayList<>();
        finalFilters.add(new MultiRowRangeFilter(rowRanges));
        
        FilterList filterList = new FilterList(finalFilters);
        Scan scan = new Scan();
        scan.setCaching(1000);
        scan.setFilter(filterList);
        
        int size = 0;
        try (ResultScanner resultScanner = hTable.getScanner(scan)) {
            for (Result ignored : resultScanner) {
                size++;
            }
        } catch (IOException e) {
            System.err.println("[XZOrderingCountQueryPlanner] Error executing count query: " + e.getMessage());
        }
        return new Tuple2<>(rowRanges.size(), size);
    }
}
