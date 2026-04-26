package query;

import config.TableConfig;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.filter.MultiRowRangeFilter;
import scala.Tuple2;

import java.io.IOException;
import java.util.List;

/**
 * Count-oriented planner that reuses QueryPlanner row-key generation.
 */
public class CountPlanner extends QueryPlanner {

    public CountPlanner(List<FilterBase> filters, TableConfig tableConfig, String tableName) throws IOException {
        super(filters, tableConfig, tableName);
    }

    @Override
    public Tuple2<Integer, ResultScanner> execute() {
        selectPrimaryFilter();
        IndexRangeResult indexResult = getKeyRangesWithIndexCount();
        List<MultiRowRangeFilter.RowRange> rowRanges = indexResult.rowRanges;

        if (rowRanges.isEmpty()) {
            lastRowRangeCount = 0;
            lastComputedRowRanges = new java.util.ArrayList<>();
            return null;
        }

        lastRowRangeCount = rowRanges.size();
        lastComputedRowRanges = new java.util.ArrayList<>(rowRanges);

        Scan scan = buildScan(rowRanges, false);
        scan.setFilter(new org.apache.hadoop.hbase.filter.FilterList(
                new MultiRowRangeFilter(rowRanges),
                new FirstKeyOnlyFilter()
        ));

        ResultScanner resultScanner = null;
        try {
            resultScanner = hTable.getScanner(scan);
        } catch (IOException e) {
            System.err.println("[CountPlanner] Error executing count query: " + e.getMessage());
        }
        return new Tuple2<>(indexResult.ranges.size(), resultScanner);
    }

    public ResultScanner executeByRowRanges(List<MultiRowRangeFilter.RowRange> rowRanges) {
        if (rowRanges == null || rowRanges.isEmpty()) {
            lastRowRangeCount = 0;
            lastComputedRowRanges = new java.util.ArrayList<>();
            return null;
        }

        lastRowRangeCount = rowRanges.size();
        lastComputedRowRanges = new java.util.ArrayList<>(rowRanges);

        Scan scan = new Scan();
        scan.setCaching(1000);
        scan.setFilter(new org.apache.hadoop.hbase.filter.FilterList(
                new MultiRowRangeFilter(rowRanges),
                new FirstKeyOnlyFilter()
        ));

        try {
            return hTable.getScanner(scan);
        } catch (IOException e) {
            System.err.println("[CountPlanner] Error executing reused count query: " + e.getMessage());
            return null;
        }
    }
}
