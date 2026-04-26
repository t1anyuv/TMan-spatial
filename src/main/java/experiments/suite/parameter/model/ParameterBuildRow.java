package experiments.suite.parameter.model;

import java.util.Locale;

public class ParameterBuildRow {
    private final String dataset;
    private final String tableName;
    private final String study;
    private final int resolution;
    private final int minTraj;
    private final long trajectoryCount;
    private final long nodeCount;
    private final long shapeCount;
    private final long buildTimeMs;
    private final long mainTableBytes;
    private final long indexTableBytes;
    private final long extraIndexInfoBytes;
    private final long totalBytes;
    private final String note;

    public ParameterBuildRow(String dataset,
                             String tableName,
                             String study,
                             int resolution,
                             int minTraj,
                             long trajectoryCount,
                             long nodeCount,
                             long shapeCount,
                             long buildTimeMs,
                             long mainTableBytes,
                             long indexTableBytes,
                             long extraIndexInfoBytes,
                             long totalBytes,
                             String note) {
        this.dataset = dataset;
        this.tableName = tableName;
        this.study = study;
        this.resolution = resolution;
        this.minTraj = minTraj;
        this.trajectoryCount = trajectoryCount;
        this.nodeCount = nodeCount;
        this.shapeCount = shapeCount;
        this.buildTimeMs = buildTimeMs;
        this.mainTableBytes = mainTableBytes;
        this.indexTableBytes = indexTableBytes;
        this.extraIndexInfoBytes = extraIndexInfoBytes;
        this.totalBytes = totalBytes;
        this.note = note == null ? "" : note;
    }

    public String toCsvRow() {
        return String.format(Locale.US, "%s,%s,%s,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%s",
                dataset,
                tableName,
                study,
                resolution,
                minTraj,
                trajectoryCount,
                nodeCount,
                shapeCount,
                buildTimeMs,
                mainTableBytes,
                indexTableBytes,
                extraIndexInfoBytes,
                totalBytes,
                csv(note));
    }

    private static String csv(String value) {
        return "\"" + value.replace("\"", "\"\"") + "\"";
    }
}
