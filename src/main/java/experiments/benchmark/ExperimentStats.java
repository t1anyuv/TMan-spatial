package experiments.benchmark;

import lombok.Getter;

import java.util.Locale;

/**
 * 实验统计结果
 * <p>
 * 指标说明：
 * <ul>
 *   <li><b>LogicIndexRanges</b>（逻辑索引区间数）：SFC 上 {@code IndexRange} 的条数，由曲线算法直接产生。</li>
 *   <li><b>RowKeyRanges</b>（行键扫描区间数）：交给 HBase {@code MultiRowRangeFilter} 的 RowRange 条数；
 *       次索引路径下常远大于逻辑区间数（Redis 展开）。</li>
 *   <li><b>candidates</b>：扫描后、空间过滤前的候选轨迹数。</li>
 *   <li><b>finalSize</b>：空间过滤后的结果数。</li>
 *   <li><b>VC</b>（Visited Cells）：访问代价近似，逻辑 range 上 contained=true 计 1，否则按索引值宽度累计。</li>
 * </ul>
 */
@Getter
public class ExperimentStats {

    public static class StatValues {
        private long min = Long.MAX_VALUE;
        private long max = Long.MIN_VALUE;
        private long sum = 0L;
        private int count = 0;

        public void add(long value) {
            min = Math.min(min, value);
            max = Math.max(max, value);
            sum += value;
            count++;
        }

        /**
         * 合并一个已聚合的统计块（min/max/avg/count）
         */
        public void addAggregated(long min, long max, long avg, int count) {
            if (count <= 0) {
                return;
            }
            this.min = Math.min(this.min, min);
            this.max = Math.max(this.max, max);
            this.sum += avg * count;
            this.count += count;
        }

        public long getMin() {
            if (count == 0) {
                return 0;
            }
            return min;
        }

        public long getMax() {
            if (count == 0) {
                return 0;
            }
            return max;
        }

        public long getAvg() {
            if (count == 0) {
                return 0;
            }
            return sum / count;
        }

        /** 算术平均（浮点），用于 VC 等在聚合后仍可能为小数的指标。 */
        public double getAvgDouble() {
            if (count == 0) {
                return 0.0;
            }
            return (double) sum / count;
        }

        public int getCount() {
            return count;
        }
    }

    private final IndexMethod method;
    private final DatasetConfig dataset;
    private final StatValues latencyStats = new StatValues();
    /** 逻辑 {@code IndexRange} 条数（SFC 区间） */
    private final StatValues logicIndexRangeStats = new StatValues();
    /** HBase 扫描 {@code RowRange} 条数 */
    private final StatValues rowKeyRangeStats = new StatValues();
    private final StatValues candidatesStats = new StatValues();
    private final StatValues visitedCellsStats = new StatValues();
    private final StatValues finalResultStats = new StatValues();

    public ExperimentStats(IndexMethod method, DatasetConfig dataset) {
        this.method = method;
        this.dataset = dataset;
    }

    public void addQueryResult(QueryResult result) {
        latencyStats.add(result.getLatencyMs());
        visitedCellsStats.add(result.getVisitedCells());
        logicIndexRangeStats.add(result.getCandidateRangeInterval());
        finalResultStats.add(result.getFinalResultCount());
    }

    /**
     * 获取CSV格式的统计行（对比实验总表）
     */
    public String toCsvRow() {
        return String.format(Locale.US,
                "%s,%s,%s,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%.2f,%d",
                method.getShortName(),
                dataset.getDatasetName(),
                dataset.getDistribution(),
                dataset.getQueryRange(),
                latencyStats.getAvg(),
                latencyStats.getMin(),
                latencyStats.getMax(),
                logicIndexRangeStats.getAvg(),
                rowKeyRangeStats.getAvg(),
                candidatesStats.getAvg(),
                finalResultStats.getAvg(),
                finalResultStats.getMin(),
                finalResultStats.getMax(),
                visitedCellsStats.getAvgDouble(),
                latencyStats.getCount()
        );
    }

    public static String getCsvHeader() {
        return "Method,Dataset,Distribution,QueryRange_Meters,Latency_Avg,Latency_Min,Latency_Max,"
                + "LogicIndexRanges_Avg,RowKeyRanges_Avg,Candidates_Avg,FinalSize_Avg,FinalSize_Min,FinalSize_Max,"
                + "VC_Avg,QueryCount";
    }
}
