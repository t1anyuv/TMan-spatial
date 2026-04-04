package experiments.benchmark.model;

import experiments.benchmark.config.DatasetConfig;
import experiments.benchmark.config.IndexMethod;
import lombok.Getter;
import lombok.Setter;

/**
 * 实验统计结果
 */
@Getter
public class ExperimentStats {

    public static class StatValues {
        private long min = Long.MAX_VALUE;
        private long max = Long.MIN_VALUE;
        private long sum = 0L;
        @Getter
        private int count = 0;

        public void add(long value) {
            min = Math.min(min, value);
            max = Math.max(max, value);
            sum += value;
            count++;
        }

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
            return count == 0 ? 0 : min;
        }

        public long getMax() {
            return count == 0 ? 0 : max;
        }

        public long getAvg() {
            return count == 0 ? 0 : sum / count;
        }

        public double getAvgDouble() {
            return count == 0 ? 0.0 : (double) sum / count;
        }
    }

    private final IndexMethod method;
    private final DatasetConfig dataset;
    private final StatValues latencyStats = new StatValues();
    private final StatValues logicIndexRangeStats = new StatValues();
    private final StatValues quadCodeRangeStats = new StatValues();
    private final StatValues qOrderRangeStats = new StatValues();
    private final StatValues rowKeyRangeStats = new StatValues();
    private final StatValues candidatesStats = new StatValues();
    private final StatValues visitedCellsStats = new StatValues();
    private final StatValues finalResultStats = new StatValues();
    private final StatValues redisAccessCountStats = new StatValues();
    private final StatValues redisShapeFilterRateScaledStats = new StatValues();
    @Setter
    private long indexSizeKb = 0L;

    public ExperimentStats(IndexMethod method, DatasetConfig dataset) {
        this.method = method;
        this.dataset = dataset;
    }
}
