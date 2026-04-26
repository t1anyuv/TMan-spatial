package experiments.common.model;

import lombok.Getter;

@Getter
public class QueryMetrics {
    private final long latencyMs;
    private final long rki;
    private final long ct;
    private final long finalCount;

    public QueryMetrics(long latencyMs, long rki, long ct, long finalCount) {
        this.latencyMs = latencyMs;
        this.rki = rki;
        this.ct = ct;
        this.finalCount = finalCount;
    }
}
