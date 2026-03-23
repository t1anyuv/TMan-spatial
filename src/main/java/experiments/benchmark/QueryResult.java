package experiments.benchmark;

import lombok.Getter;

/**
 * 单个查询结果统计
 * 包含四个指标：Latency, Visited Cells, Candidate Range Intervals, Final Result Count
 */
@Getter
public class QueryResult {
    private final long latencyMs;           // 查询延迟（毫秒）
    private final long visitedCells;        // 访问的单元格数
    private final long candidateRangeInterval;   // 候选范围区间数
    private final long finalResultCount;    // 最终返回的轨迹数量
    
    public QueryResult(long latencyMs, long visitedCells, long candidateRangeInterval, long finalResultCount) {
        this.latencyMs = latencyMs;
        this.visitedCells = visitedCells;
        this.candidateRangeInterval = candidateRangeInterval;
        this.finalResultCount = finalResultCount;
    }

    @Override
    public String toString() {
        return String.format("QueryResult{latency=%dms, visited=%d, rangeIntervals=%d, results=%d}",
            latencyMs, visitedCells, candidateRangeInterval, finalResultCount);
    }
}
