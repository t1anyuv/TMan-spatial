package experiments.suite.comparison.model;

import experiments.common.config.IndexMethod;
import lombok.Getter;

import java.util.Locale;

@Getter
public class ExperimentResult {
    private final String dataset;
    private final String distribution;
    private final int queryRange;
    private final IndexMethod method;
    private final double avgLatencyMs;
    private final double avgRki;
    private final double avgCt;
    private final double avgFinal;
    private final int queryCount;

    public ExperimentResult(String dataset,
                            String distribution,
                            int queryRange,
                            IndexMethod method,
                            double avgLatencyMs,
                            double avgRki,
                            double avgCt,
                            double avgFinal,
                            int queryCount) {
        this.dataset = dataset;
        this.distribution = distribution;
        this.queryRange = queryRange;
        this.method = method;
        this.avgLatencyMs = avgLatencyMs;
        this.avgRki = avgRki;
        this.avgCt = avgCt;
        this.avgFinal = avgFinal;
        this.queryCount = queryCount;
    }

    public String toCsvRow() {
        return String.format(Locale.US, "%s,%s,%d,%s,%.4f,%.4f,%.4f,%.4f,%d",
                dataset,
                distribution,
                queryRange,
                method.getShortName(),
                avgLatencyMs,
                avgRki,
                avgCt,
                avgFinal,
                queryCount);
    }
}
