package experiments.common.execution;

import config.TableConfig;
import experiments.common.model.QueryMetrics;
import experiments.standalone.query.BMTreeSpatialQuery;
import experiments.standalone.query.BasicQuery;
import experiments.standalone.query.LMSFCSpatialQuery;
import experiments.standalone.query.SpatialQuery;
import filter.SpatialFilter;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.filter.FilterBase;
import query.CountPlanner;
import query.QueryPlanner;
import scala.Tuple2;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

public class PlannerBackedQueryExecutor {

    public QuerySession openSession(String tableName, TableConfig tableConfig, BasicQuery queryStrategy) throws IOException {
        SpatialFilter.validateRuntimeDependencies();
        return new QuerySession(tableName, tableConfig, queryStrategy);
    }

    public void warmup(String condition, String tableName, TableConfig tableConfig, BasicQuery queryStrategy) throws IOException {
        try (QuerySession session = openSession(tableName, tableConfig, queryStrategy)) {
            session.warmup(condition);
        }
    }

    public QueryMetrics execute(String condition, String tableName, TableConfig tableConfig, BasicQuery queryStrategy) throws IOException {
        try (QuerySession session = openSession(tableName, tableConfig, queryStrategy)) {
            return session.execute(condition);
        }
    }

    public static BasicQuery createQueryStrategy(TableConfig tableConfig) {
        switch (tableConfig.getSpatialIndexKind()) {
            case LMSFC:
                return new LMSFCSpatialQuery();
            case BMTREE:
                return new BMTreeSpatialQuery();
            default:
                return new SpatialQuery();
        }
    }

    private long countScanner(ResultScanner scanner) throws IOException {
        if (scanner == null) {
            return 0L;
        }

        long count = 0L;
        try {
            for (Result ignored : scanner) {
                count++;
            }
            return count;
        } finally {
            scanner.close();
        }
    }

    public final class QuerySession implements Closeable {
        private final String tableName;
        private final TableConfig tableConfig;
        private final BasicQuery queryStrategy;
        private final QueryPlanner queryPlanner;
        private final CountPlanner countPlanner;

        private QuerySession(String tableName, TableConfig tableConfig, BasicQuery queryStrategy) throws IOException {
            this.tableName = tableName;
            this.tableConfig = tableConfig;
            this.queryStrategy = queryStrategy;
            this.queryPlanner = new QueryPlanner(null, tableConfig, tableName);
            this.countPlanner = new CountPlanner(null, tableConfig, tableName);
        }

        public void warmup(String condition) throws IOException {
            execute(condition);
        }

        public QueryMetrics execute(String condition) throws IOException {
            List<FilterBase> filters = queryStrategy.getFilters(condition, tableConfig);

            long startNs = System.nanoTime();
            Tuple2<Integer, ResultScanner> result = queryPlanner.executeByFilter(filters, tableConfig, tableName);
            long finalCount = countScanner(result == null ? null : result._2());
            long latencyMs = (System.nanoTime() - startNs) / 1_000_000L;

            long rki = queryPlanner.getLastRowRangeCount();
            long ct = countScanner(countPlanner.executeByRowRanges(queryPlanner.getLastComputedRowRanges()));
            return new QueryMetrics(latencyMs, rki, ct, finalCount);
        }

        @Override
        public void close() throws IOException {
            IOException failure = null;
            try {
                countPlanner.close();
            } catch (IOException e) {
                failure = e;
            }
            try {
                queryPlanner.close();
            } catch (IOException e) {
                if (failure == null) {
                    failure = e;
                } else {
                    failure.addSuppressed(e);
                }
            }
            if (failure != null) {
                throw failure;
            }
        }
    }
}
