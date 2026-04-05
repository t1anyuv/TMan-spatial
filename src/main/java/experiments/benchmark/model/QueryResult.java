package experiments.benchmark.model;

import lombok.Getter;

/**
 * 单个查询结果统计
 * 包含四个指标：Latency, Visited Cells, Candidate Range Intervals, Final Result Count
 */
@Getter
public class QueryResult {
    private final long latencyMs;
    private final long visitedCells;
    private final long candidateRangeInterval;
    private final long finalResultCount;
    private final long redisAccessCount;

    public QueryResult(long latencyMs, long visitedCells, long candidateRangeInterval, long finalResultCount, long redisAccessCount) {
        this.latencyMs = latencyMs;
        this.visitedCells = visitedCells;
        this.candidateRangeInterval = candidateRangeInterval;
        this.finalResultCount = finalResultCount;
        this.redisAccessCount = redisAccessCount;
    }

    @Override
    public String toString() {
        return String.format("QueryResult{latency=%dms, visited=%d, rangeIntervals=%d, results=%d}",
                latencyMs, visitedCells, candidateRangeInterval, finalResultCount);
    }
}
