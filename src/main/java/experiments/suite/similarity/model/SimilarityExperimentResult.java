package experiments.suite.similarity.model;

import experiments.common.config.IndexMethod;

import java.util.Locale;

public class SimilarityExperimentResult {
    private final String dataset;
    private final String queryType;
    private final String parameterName;
    private final String parameterValue;
    private final String distanceFunction;
    private final IndexMethod method;
    private final double avgLatencyMs;
    private final double avgRowKeyRanges;
    private final double avgCandidates;
    private final double avgFinalSize;
    private final double avgIterations;
    private final int queryCount;

    public SimilarityExperimentResult(String dataset,
                                      String queryType,
                                      String parameterName,
                                      String parameterValue,
                                      String distanceFunction,
                                      IndexMethod method,
                                      double avgLatencyMs,
                                      double avgRowKeyRanges,
                                      double avgCandidates,
                                      double avgFinalSize,
                                      double avgIterations,
                                      int queryCount) {
        this.dataset = dataset;
        this.queryType = queryType;
        this.parameterName = parameterName;
        this.parameterValue = parameterValue;
        this.distanceFunction = distanceFunction;
        this.method = method;
        this.avgLatencyMs = avgLatencyMs;
        this.avgRowKeyRanges = avgRowKeyRanges;
        this.avgCandidates = avgCandidates;
        this.avgFinalSize = avgFinalSize;
        this.avgIterations = avgIterations;
        this.queryCount = queryCount;
    }

    public String toCsvRow() {
        return String.format(Locale.US, "%s,%s,%s,%s,%s,%s,%.4f,%.4f,%.4f,%.4f,%.4f,%d",
                dataset,
                queryType,
                parameterName,
                parameterValue,
                distanceFunction,
                method.getShortName(),
                avgLatencyMs,
                avgRowKeyRanges,
                avgCandidates,
                avgFinalSize,
                avgIterations,
                queryCount);
    }
}
