package experiments.suite.ablation.model;

import java.util.Locale;

public class AblationResultRow {
    private final String variant;
    private final String orderSource;
    private final int adaptivePartition;
    private final int resolution;
    private final int minTraj;
    private final double avgLatencyMs;
    private final double avgRki;
    private final double avgCt;
    private final double avgFinal;
    private final int queryCount;

    public AblationResultRow(String variant,
                             String orderSource,
                             int adaptivePartition,
                             int resolution,
                             int minTraj,
                             double avgLatencyMs,
                             double avgRki,
                             double avgCt,
                             double avgFinal,
                             int queryCount) {
        this.variant = variant;
        this.orderSource = orderSource;
        this.adaptivePartition = adaptivePartition;
        this.resolution = resolution;
        this.minTraj = minTraj;
        this.avgLatencyMs = avgLatencyMs;
        this.avgRki = avgRki;
        this.avgCt = avgCt;
        this.avgFinal = avgFinal;
        this.queryCount = queryCount;
    }

    public String toCsvRow() {
        return String.format(Locale.US, "%s,%s,%d,%d,%d,%.4f,%.4f,%.4f,%.4f,%d",
                variant,
                orderSource,
                adaptivePartition,
                resolution,
                minTraj,
                avgLatencyMs,
                avgRki,
                avgCt,
                avgFinal,
                queryCount);
    }
}
