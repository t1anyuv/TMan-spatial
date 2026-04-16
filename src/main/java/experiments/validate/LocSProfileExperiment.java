package experiments.validate;

import config.TableConfig;
import experiments.tman.LetiSpatialQuery;
import experiments.tman.SpatialQuery;
import index.RangeStatsBridge;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.locationtech.sfcurve.IndexRange;
import query.CountPlanner;
import query.QueryPlanner;
import scala.Tuple2;
import utils.QueryUtils;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;

/**
 * Profile LETI and TShape on the same query set and persist per-query timings.
 */
public class LocSProfileExperiment {

    public static void main(String[] args) throws Exception {
        if (args.length < 4) {
            System.out.println("Usage: LocSProfileExperiment <leti_table> <tshape_table> <query_file> <output_dir> [limit]");
            System.out.println("Example: LocSProfileExperiment tdrive_leti tdrive_tshape src/main/resources/tdrive-query/range/uniform/uniform_100m.txt debug/profile 20");
            return;
        }

        String letiTable = args[0].trim();
        String tshapeTable = args[1].trim();
        String queryFile = args[2].trim();
        String outputDir = args[3].trim();
        int limit = args.length >= 5 ? Integer.parseInt(args[4].trim()) : 20;

        new LocSProfileExperiment().run(letiTable, tshapeTable, queryFile, outputDir, limit);
    }

    public void run(String letiTable, String tshapeTable, String queryFile, String outputDir, int limit) throws Exception {
        List<String> queries = loadQueries(queryFile, limit);
        if (queries.isEmpty()) {
            throw new IllegalArgumentException("No queries found in file: " + queryFile);
        }

        Path outDir = Paths.get(outputDir);
        Files.createDirectories(outDir);
        Files.write(outDir.resolve("selected_queries.txt"), queries, StandardCharsets.UTF_8);

        List<ProfileRow> rows = new ArrayList<>(queries.size() * 2);
        Path shapeOrderDir = outDir.resolve("shape_orders");
        Files.createDirectories(shapeOrderDir);

        rows.addAll(runForMethod("LETI", letiTable, queries, true, shapeOrderDir));
        rows.addAll(runForMethod("TShape", tshapeTable, queries, false, shapeOrderDir));

        writeDetails(rows, outDir.resolve("profile_details.csv"));
        writeSummary(rows, outDir.resolve("profile_summary.csv"));
        printContainedIntersectComparison(rows);

        System.out.println("Profile finished.");
        System.out.println("  queries        : " + queries.size());
        System.out.println("  details        : " + outDir.resolve("profile_details.csv"));
        System.out.println("  summary        : " + outDir.resolve("profile_summary.csv"));
        System.out.println("  selectedQueries: " + outDir.resolve("selected_queries.txt"));
        System.out.println("  shapeOrders    : " + shapeOrderDir);
    }

    private List<ProfileRow> runForMethod(String method, String tableName, List<String> queries, boolean leti, Path shapeOrderDir) throws Exception {
        QueryUtils queryUtils = new QueryUtils();
        TableConfig config = queryUtils.getTableConfig(tableName + "_meta");
        SpatialQuery queryBuilder = leti ? new LetiSpatialQuery() : new SpatialQuery();

        try (QueryPlanner queryPlanner = new QueryPlanner(null, config, tableName);
             CountPlanner countPlanner = new CountPlanner(null, config, tableName)) {

            warmup(queryBuilder, queryPlanner, countPlanner, config, tableName, queries.get(0));

            List<ProfileRow> rows = new ArrayList<>(queries.size());
            for (int i = 0; i < queries.size(); i++) {
                rows.add(profileOne(method, tableName, queries.get(i), i, leti, queryBuilder, queryPlanner, countPlanner, config, shapeOrderDir));
            }
            return rows;
        }
    }

    private void warmup(SpatialQuery queryBuilder,
                        QueryPlanner queryPlanner,
                        CountPlanner countPlanner,
                        TableConfig config,
                        String tableName,
                        String query) throws IOException {
        List<FilterBase> filters = queryBuilder.getFilters(query, config);
        Tuple2<Integer, ResultScanner> finalResult = queryPlanner.executeByFilter(filters, config, tableName);
        countScanner(finalResult == null ? null : finalResult._2());
        ResultScanner candidateResult = countPlanner.executeByRowRanges(queryPlanner.getLastComputedRowRanges());
        countScanner(candidateResult);
    }

    private ProfileRow profileOne(String method,
                                  String tableName,
                                  String query,
                                  int queryIndex,
                                  boolean leti,
                                  SpatialQuery queryBuilder,
                                  QueryPlanner queryPlanner,
                                  CountPlanner countPlanner,
                                  TableConfig config,
                                  Path shapeOrderDir) throws IOException {
        ProfileRow row = new ProfileRow();
        row.method = method;
        row.tableName = tableName;
        row.queryIndex = queryIndex;
        row.query = query;

        long filterBuildStartNs = System.nanoTime();
        List<FilterBase> filters = queryBuilder.getFilters(query, config);
        row.filterBuildMs = nanosToMillis(System.nanoTime() - filterBuildStartNs);

        long finalStartNs = System.nanoTime();
        Tuple2<Integer, ResultScanner> finalResult = queryPlanner.executeByFilter(filters, config, tableName);
        row.finalIndexRangeComputeMs = nanosToMillis(queryPlanner.getLastIndexRangeComputeNs());
        row.finalRowRangeBuildMs = nanosToMillis(queryPlanner.getLastRowRangeBuildNs());
        row.finalScannerOpenMs = nanosToMillis(queryPlanner.getLastScannerOpenNs());
        long finalIterateStartNs = System.nanoTime();
        row.finalSize = countScanner(finalResult == null ? null : finalResult._2());
        row.finalIterateMs = nanosToMillis(System.nanoTime() - finalIterateStartNs);
        row.finalTotalMs = nanosToMillis(System.nanoTime() - finalStartNs);

        row.logicIndexRanges = finalResult == null ? 0 : finalResult._1();
        row.quadCodeRanges = queryPlanner.getLastQuadCodeRangeCount();
        row.qOrderRanges = queryPlanner.getLastQOrderRangeCount();
        row.rowKeyRanges = queryPlanner.getLastRowRangeCount();
        row.visitedCells = queryPlanner.getLastVisitedCells();
        row.redisAccessCount = queryPlanner.getLastRedisAccessCount();
        row.redisShapeFilterRateScaled = queryPlanner.getLastRedisShapeFilterRateScaled();
        RangeStatsBridge.Stats rangeStats = RangeStatsBridge.getLast(leti ? RangeStatsBridge.Kind.LETI : RangeStatsBridge.Kind.TSHAPE);
        row.containedQuadCount = rangeStats == null ? 0L : rangeStats.containedQuadCount;
        row.intersectQuadCount = rangeStats == null ? 0L : rangeStats.intersectQuadCount;
        row.shapeOrderDump = writeShapeOrderDump(shapeOrderDir, row, queryPlanner.getLastLogicalIndexRanges(), config, leti);

        long candidateStartNs = System.nanoTime();
        ResultScanner candidateResult = countPlanner.executeByRowRanges(queryPlanner.getLastComputedRowRanges());
        row.candidateIndexRangeComputeMs = nanosToMillis(countPlanner.getLastIndexRangeComputeNs());
        row.candidateRowRangeBuildMs = nanosToMillis(countPlanner.getLastRowRangeBuildNs());
        row.candidateScannerOpenMs = nanosToMillis(countPlanner.getLastScannerOpenNs());
        long candidateIterateStartNs = System.nanoTime();
        row.candidates = countScanner(candidateResult);
        row.candidateIterateMs = nanosToMillis(System.nanoTime() - candidateIterateStartNs);
        row.candidateTotalMs = nanosToMillis(System.nanoTime() - candidateStartNs);

        row.endToEndMs = row.filterBuildMs + row.finalTotalMs + row.candidateTotalMs;
        return row;
    }

    private String writeShapeOrderDump(Path shapeOrderDir,
                                       ProfileRow row,
                                       List<IndexRange> logicalRanges,
                                       TableConfig config,
                                       boolean leti) throws IOException {
        Files.createDirectories(shapeOrderDir);
        String prefixName = leti ? "qOrder" : "quadCode";
        int moveBits = resolveMoveBits(config, leti);
        long shapeMask = lowBitsMask(moveBits);

        Map<Long, List<Long>> shapeOrdersByPrefix = new TreeMap<>();
        List<String> containedLines = new ArrayList<>();

        if (logicalRanges != null) {
            for (IndexRange range : logicalRanges) {
                long lower = range.lower();
                long upper = range.upper();
                if (lower == upper) {
                    long prefix = lower >>> moveBits;
                    long shapeOrder = lower & shapeMask;
                    shapeOrdersByPrefix.computeIfAbsent(prefix, ignored -> new ArrayList<>()).add(shapeOrder);
                } else {
                    long startPrefix = lower >>> moveBits;
                    long endPrefix = upper >>> moveBits;
                    long startShapeOrder = lower & shapeMask;
                    long endShapeOrder = upper & shapeMask;
                    containedLines.add(String.format(
                            Locale.US,
                            "%d,%s,%s,%d,%d,%d,%d,%d,%d",
                            row.queryIndex,
                            row.method,
                            range.contained() ? "contained" : "range",
                            startPrefix,
                            endPrefix,
                            startShapeOrder,
                            endShapeOrder,
                            lower,
                            upper
                    ));
                }
            }
        }

        Path output = shapeOrderDir.resolve(String.format(Locale.US, "query_%03d_%s_shape_orders.csv", row.queryIndex, row.method.toLowerCase(Locale.ROOT)));
        try (BufferedWriter writer = Files.newBufferedWriter(output, StandardCharsets.UTF_8)) {
            writer.write("queryIndex,method,type,prefixName,prefix,shapeOrderCount,shapeOrders");
            writer.newLine();
            for (Map.Entry<Long, List<Long>> entry : shapeOrdersByPrefix.entrySet()) {
                List<Long> shapeOrders = entry.getValue();
                Collections.sort(shapeOrders);
                writer.write(String.format(
                        Locale.US,
                        "%d,%s,intersect,%s,%d,%d,\"%s\"",
                        row.queryIndex,
                        row.method,
                        prefixName,
                        entry.getKey(),
                        shapeOrders.size(),
                        joinLongs(shapeOrders)
                ));
                writer.newLine();
            }
            if (!containedLines.isEmpty()) {
                writer.newLine();
                writer.write("queryIndex,method,type,startPrefix,endPrefix,startShapeOrder,endShapeOrder,rangeLower,rangeUpper");
                writer.newLine();
                for (String containedLine : containedLines) {
                    writer.write(containedLine);
                    writer.newLine();
                }
            }
        }
        return output.toString();
    }

    private int resolveMoveBits(TableConfig config, boolean leti) {
        if (leti && config.isAdaptivePartition() && config.getMaxShapeBits() > 0) {
            return config.getMaxShapeBits();
        }
        return config.getAlpha() * config.getBeta();
    }

    private long lowBitsMask(int bits) {
        if (bits <= 0) {
            return 0L;
        }
        if (bits >= 63) {
            return -1L;
        }
        return (1L << bits) - 1L;
    }

    private String joinLongs(List<Long> values) {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < values.size(); i++) {
            if (i > 0) {
                builder.append(' ');
            }
            builder.append(values.get(i));
        }
        return builder.toString();
    }

    private List<String> loadQueries(String queryFile, int limit) throws IOException {
        List<String> lines = Files.readAllLines(Paths.get(queryFile), StandardCharsets.UTF_8);
        List<String> queries = new ArrayList<>();
        for (String line : lines) {
            String trimmed = line == null ? "" : line.trim();
            if (!trimmed.isEmpty()) {
                queries.add(trimmed);
            }
            if (limit > 0 && queries.size() >= limit) {
                break;
            }
        }
        return queries;
    }

    private long countScanner(ResultScanner scanner) {
        if (scanner == null) {
            return 0L;
        }
        long count = 0L;
        try {
            for (Result ignored : scanner) {
                count++;
            }
        } finally {
            scanner.close();
        }
        return count;
    }

    private void writeDetails(List<ProfileRow> rows, Path output) throws IOException {
        try (BufferedWriter writer = Files.newBufferedWriter(output, StandardCharsets.UTF_8)) {
            writer.write("queryIndex,method,tableName,query,finalIndexRangeComputeMs,finalIterateMs,finalTotalMs,candidateTotalMs,endToEndMs,logicIndexRanges,rowKeyRanges,visitedCells,containedQuadCount,intersectQuadCount,redisAccessCount,redisShapeFilterRateScaled,candidates,finalSize");
            writer.newLine();
            for (ProfileRow row : rows) {
                writer.write(row.toCompactCsv());
                writer.newLine();
            }
        }
    }

    private void printContainedIntersectComparison(List<ProfileRow> rows) {
        Map<Integer, Map<String, ProfileRow>> grouped = new LinkedHashMap<>();
        for (ProfileRow row : rows) {
            grouped.computeIfAbsent(row.queryIndex, ignored -> new LinkedHashMap<>()).put(row.method, row);
        }

        System.out.println("Contained / Intersect comparison");
        System.out.println("queryIndex | LETI(contained, intersect, logicRanges) | TShape(contained, intersect, logicRanges)");
        for (Map.Entry<Integer, Map<String, ProfileRow>> entry : grouped.entrySet()) {
            ProfileRow leti = entry.getValue().get("LETI");
            ProfileRow tshape = entry.getValue().get("TShape");
            String letiPart = leti == null
                    ? "N/A"
                    : leti.containedQuadCount + ", " + leti.intersectQuadCount + ", " + leti.logicIndexRanges;
            String tshapePart = tshape == null
                    ? "N/A"
                    : tshape.containedQuadCount + ", " + tshape.intersectQuadCount + ", " + tshape.logicIndexRanges;
            System.out.printf(Locale.US, "%10d | %-39s | %s%n", entry.getKey(), letiPart, tshapePart);
        }
    }

    private void writeSummary(List<ProfileRow> rows, Path output) throws IOException {
        Map<String, Map<String, List<Double>>> grouped = new LinkedHashMap<>();
        for (ProfileRow row : rows) {
            Map<String, Double> metrics = row.toMetricMap();
            Map<String, List<Double>> metricMap = grouped.computeIfAbsent(row.method, ignored -> new LinkedHashMap<>());
            for (Map.Entry<String, Double> entry : metrics.entrySet()) {
                metricMap.computeIfAbsent(entry.getKey(), ignored -> new ArrayList<>()).add(entry.getValue());
            }
        }

        try (BufferedWriter writer = Files.newBufferedWriter(output, StandardCharsets.UTF_8)) {
            writer.write("method,metric,min,max,avg,p50,p90");
            writer.newLine();
            for (Map.Entry<String, Map<String, List<Double>>> methodEntry : grouped.entrySet()) {
                for (Map.Entry<String, List<Double>> metricEntry : methodEntry.getValue().entrySet()) {
                    List<Double> values = metricEntry.getValue();
                    if (values.isEmpty() || allZero(values)) {
                        continue;
                    }
                    Collections.sort(values);
                    writer.write(String.format(
                            Locale.US,
                            "%s,%s,%.3f,%.3f,%.3f,%.3f,%.3f",
                            methodEntry.getKey(),
                            metricEntry.getKey(),
                            values.get(0),
                            values.get(values.size() - 1),
                            average(values),
                            percentile(values, 0.50),
                            percentile(values, 0.90)
                    ));
                    writer.newLine();
                }
            }
        }
    }

    private double average(List<Double> values) {
        double sum = 0.0;
        for (Double value : values) {
            sum += value;
        }
        return values.isEmpty() ? 0.0 : sum / values.size();
    }

    private boolean allZero(List<Double> values) {
        for (Double value : values) {
            if (Math.abs(value) > 1e-9) {
                return false;
            }
        }
        return true;
    }

    private double percentile(List<Double> values, double ratio) {
        if (values.isEmpty()) {
            return 0.0;
        }
        int index = (int) Math.ceil(values.size() * ratio) - 1;
        index = Math.max(0, Math.min(index, values.size() - 1));
        return values.get(index);
    }

    private double nanosToMillis(long nanos) {
        return nanos / 1_000_000.0;
    }

    private static final class ProfileRow {
        int queryIndex;
        String method;
        String tableName;
        String query;
        double filterBuildMs;
        double finalIndexRangeComputeMs;
        double finalRowRangeBuildMs;
        double finalScannerOpenMs;
        double finalIterateMs;
        double finalTotalMs;
        double candidateIndexRangeComputeMs;
        double candidateRowRangeBuildMs;
        double candidateScannerOpenMs;
        double candidateIterateMs;
        double candidateTotalMs;
        double endToEndMs;
        long logicIndexRanges;
        long quadCodeRanges;
        long qOrderRanges;
        long rowKeyRanges;
        long visitedCells;
        long containedQuadCount;
        long intersectQuadCount;
        long redisAccessCount;
        long redisShapeFilterRateScaled;
        long candidates;
        long finalSize;
        String shapeOrderDump;

        String toCompactCsv() {
            return String.format(
                    Locale.US,
                    "%d,%s,%s,%s,%.3f,%.3f,%.3f,%.3f,%.3f,%d,%d,%d,%d,%d,%d,%d,%d,%d",
                    queryIndex,
                    method,
                    tableName,
                    quote(query),
                    finalIndexRangeComputeMs,
                    finalIterateMs,
                    finalTotalMs,
                    candidateTotalMs,
                    endToEndMs,
                    logicIndexRanges,
                    rowKeyRanges,
                    visitedCells,
                    containedQuadCount,
                    intersectQuadCount,
                    redisAccessCount,
                    redisShapeFilterRateScaled,
                    candidates,
                    finalSize
            );
        }

        Map<String, Double> toMetricMap() {
            Map<String, Double> metrics = new LinkedHashMap<>();
            metrics.put("finalIndexRangeComputeMs", finalIndexRangeComputeMs);
            metrics.put("finalIterateMs", finalIterateMs);
            metrics.put("finalTotalMs", finalTotalMs);
            metrics.put("candidateTotalMs", candidateTotalMs);
            metrics.put("endToEndMs", endToEndMs);
            metrics.put("logicIndexRanges", (double) logicIndexRanges);
            metrics.put("rowKeyRanges", (double) rowKeyRanges);
            metrics.put("visitedCells", (double) visitedCells);
            metrics.put("containedQuadCount", (double) containedQuadCount);
            metrics.put("intersectQuadCount", (double) intersectQuadCount);
            metrics.put("redisAccessCount", (double) redisAccessCount);
            metrics.put("redisShapeFilterRateScaled", (double) redisShapeFilterRateScaled);
            metrics.put("candidates", (double) candidates);
            metrics.put("finalSize", (double) finalSize);
            return metrics;
        }

        private String quote(String value) {
            return "\"" + value.replace("\"", "\"\"") + "\"";
        }
    }
}
