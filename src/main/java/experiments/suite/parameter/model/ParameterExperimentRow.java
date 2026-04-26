package experiments.suite.parameter.model;

import java.util.Locale;

public class ParameterExperimentRow {
    private final String dataset;
    private final String study;
    private final int resolution;
    private final int minTraj;
    private final long rc;
    private final double avgLatencyMs;
    private final double avgRki;
    private final double avgCt;
    private final int queryCount;

    public ParameterExperimentRow(String dataset,
                                  String study,
                                  int resolution,
                                  int minTraj,
                                  long rc,
                                  double avgLatencyMs,
                                  double avgRki,
                                  double avgCt,
                                  int queryCount) {
        this.dataset = dataset;
        this.study = study;
        this.resolution = resolution;
        this.minTraj = minTraj;
        this.rc = rc;
        this.avgLatencyMs = avgLatencyMs;
        this.avgRki = avgRki;
        this.avgCt = avgCt;
        this.queryCount = queryCount;
    }

    public String toCsvRow() {
        return String.format(Locale.US, "%s,%s,%d,%d,%d,%.4f,%.4f,%.4f,%d",
                dataset,
                study,
                resolution,
                minTraj,
                rc,
                avgLatencyMs,
                avgRki,
                avgCt,
                queryCount);
    }
}
