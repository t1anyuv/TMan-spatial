package experiments.standalone.query;

import entity.Trajectory;
import experiments.common.io.ExperimentPaths;
import org.apache.hadoop.hbase.util.SortedList;
import scala.Tuple7;
import similarity.TrajectorySimilarity;

import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Locale;

public class SimilarityQuery implements AutoCloseable {
    private static final int PREVIEW_RESULT_LIMIT = 5;
    private static final double ZERO_DISTANCE_EPSILON = 1e-9d;
    private static final double DISTANCE_COMPARE_EPSILON = 1e-9d;

    public static void main(String[] args) throws IOException {
        if (args.length < 6) {
            System.err.println("Usage: SimilarityQuery <table> <query_traj_file> <query_limit> <threshold> <distance_func> <result_csv> [check_mode]");
            System.err.println("distance_func: 0=Frechet, 1=Hausdorff, 2=DTW");
            System.err.println("check_mode: true/false, default=false; when true, compare indexed results with brute-force full scan");
            return;
        }

        String tableName = args[0];
        String queryTrajectoryFile = args[1];
        int queryLimit = Integer.parseInt(args[2]);
        double threshold = Double.parseDouble(args[3]);
        int func = Integer.parseInt(args[4]);
        String resultPath = args[5];
        boolean checkMode = args.length >= 7 && Boolean.parseBoolean(args[6]);

        if (queryLimit < 0) {
            throw new IllegalArgumentException("query_limit must be >= 0");
        }

        SortedList<Long> timeStatistic = new SortedList<>(Long::compare);
        SortedList<Long> sizeStatistic = new SortedList<>(Long::compare);

        try (TrajectorySimilarityQuerySupport support = new TrajectorySimilarityQuerySupport();
             FileWriter writer = new FileWriter(resultPath)) {
            System.out.println("================================================================================");
            System.out.println("Similarity Query Started");
            System.out.println("================================================================================");
            System.out.printf("Table         : %s%n", tableName);
            System.out.printf("Query File    : %s%n", queryTrajectoryFile);
            System.out.printf("Query Limit   : %d%n", queryLimit);
            System.out.printf("Threshold     : %s%n", threshold);
            System.out.printf("Distance Func : %s%n", TrajectorySimilarity.functionName(func));
            System.out.printf("Result File   : %s%n", resultPath);
            System.out.printf("Check Mode    : %s%n", checkMode);
            System.out.println("================================================================================");
            int bruteForcePassCount = 0;
            int warningCount = 0;
            SortedList<Long> rowKeyRangeStatistic = new SortedList<>(Long::compare);
            SortedList<Long> candidatesStatistic = new SortedList<>(Long::compare);

            int processedQueries = 0;
            for (String line : ExperimentPaths.readAllLines(queryTrajectoryFile)) {
                if (processedQueries >= queryLimit) {
                    break;
                }
                if (line.trim().isEmpty()) {
                    continue;
                }
                Trajectory query = TrajectorySimilarityQuerySupport.parseQueryTrajectory(line.trim());
                int queryIndex = processedQueries + 1;
                System.out.println("--------------------------------------------------------------------------------");
                System.out.printf("Query %d%n", queryIndex);
                System.out.printf("  OID/TID      : %s / %s%n", query.getOid(), query.getTid());
                System.out.printf("  Point Count  : %d%n", query.getNumGeometries());
                System.out.printf("  Query WKT    : %s%n", previewText(query.toText(), 160));
                long start = System.currentTimeMillis();
                TrajectorySimilarityQuerySupport.SimilarityQueryResult queryResult =
                        support.similarityQueryWithStats(tableName, query, threshold, func);
                long elapsed = System.currentTimeMillis() - start;
                List<TrajectorySimilarityQuerySupport.ScoredTrajectory> result = queryResult.getResults();
                timeStatistic.add(elapsed);
                sizeStatistic.add((long) result.size());
                rowKeyRangeStatistic.add((long) queryResult.getRowKeyRangeCount());
                candidatesStatistic.add(queryResult.getCandidateCount());
                System.out.printf("  Query Time   : %d ms%n", elapsed);
                System.out.printf("  Row Ranges   : %d%n", queryResult.getRowKeyRangeCount());
                System.out.printf("  Candidates   : %d%n", queryResult.getCandidateCount());
                System.out.printf("  Result Count : %d%n", result.size());
                if (result.isEmpty()) {
                    System.out.println("  Result Preview: no match under threshold");
                } else {
                    System.out.println("  Result Preview:");
                    for (int i = 0; i < Math.min(PREVIEW_RESULT_LIMIT, result.size()); i++) {
                        TrajectorySimilarityQuerySupport.ScoredTrajectory scored = result.get(i);
                        System.out.printf(
                                Locale.ROOT,
                                "    [%d] %s / %s distance=%.6f points=%d%n",
                                i + 1,
                                scored.trajectory.getOid(),
                                scored.trajectory.getTid(),
                                scored.distance,
                                scored.trajectory.getNumGeometries()
                        );
                    }
                }

                ValidationSummary validation = null;
                if (checkMode) {
                    List<TrajectorySimilarityQuerySupport.ScoredTrajectory> bruteForceResult =
                            support.bruteForceSimilarityQuery(tableName, query, threshold, func);
                    validation = validateAgainstBruteForce(result, bruteForceResult);
                    if (validation.passed) {
                        bruteForcePassCount++;
                    }
                    if (!validation.passed) {
                        warningCount++;
                    }
                    System.out.printf("  Validation   : %s%n", validation.passed ? "PASS" : "WARN");
                    System.out.printf("  Indexed Size : %d%n", validation.indexedResultSize);
                    System.out.printf("  Brute Size   : %d%n", validation.bruteForceResultSize);
                    System.out.printf("  Missing      : %d%n", validation.missingCount);
                    System.out.printf("  Extra        : %d%n", validation.extraCount);
                    System.out.printf("  Mismatch     : %d%n", validation.distanceMismatchCount);
                    if (!validation.message.isEmpty()) {
                        System.out.printf("  Check Detail : %s%n", validation.message);
                    }
                }
                processedQueries++;
            }

            writer.write("type,min,max,avg,mid,per70,per80,per90\n");
            Tuple7<Long, Long, Long, Long, Long, Long, Long> time = BasicQuery.getStatistic(timeStatistic);
            writer.write(String.format("time,%s,%s,%s,%s,%s,%s,%s\n",
                    time._1(), time._2(), time._3(), time._4(), time._5(), time._6(), time._7()));
            Tuple7<Long, Long, Long, Long, Long, Long, Long> rowKeyRanges = BasicQuery.getStatistic(rowKeyRangeStatistic);
            writer.write(String.format("rowKeyRanges,%s,%s,%s,%s,%s,%s,%s\n",
                    rowKeyRanges._1(), rowKeyRanges._2(), rowKeyRanges._3(), rowKeyRanges._4(),
                    rowKeyRanges._5(), rowKeyRanges._6(), rowKeyRanges._7()));
            Tuple7<Long, Long, Long, Long, Long, Long, Long> candidates = BasicQuery.getStatistic(candidatesStatistic);
            writer.write(String.format("candidates,%s,%s,%s,%s,%s,%s,%s\n",
                    candidates._1(), candidates._2(), candidates._3(), candidates._4(),
                    candidates._5(), candidates._6(), candidates._7()));
            Tuple7<Long, Long, Long, Long, Long, Long, Long> size = BasicQuery.getStatistic(sizeStatistic);
            writer.write(String.format("finalSize,%s,%s,%s,%s,%s,%s,%s\n",
                    size._1(), size._2(), size._3(), size._4(), size._5(), size._6(), size._7()));

            System.out.println("================================================================================");
            System.out.println("Similarity Query Summary");
            System.out.println("================================================================================");
            System.out.printf("Processed Queries : %d%n", processedQueries);
            System.out.printf("Time Stats (ms)   : min=%s max=%s avg=%s mid=%s p70=%s p80=%s p90=%s%n",
                    time._1(), time._2(), time._3(), time._4(), time._5(), time._6(), time._7());
            System.out.printf("Row Ranges       : min=%s max=%s avg=%s mid=%s p70=%s p80=%s p90=%s%n",
                    rowKeyRanges._1(), rowKeyRanges._2(), rowKeyRanges._3(), rowKeyRanges._4(),
                    rowKeyRanges._5(), rowKeyRanges._6(), rowKeyRanges._7());
            System.out.printf("Candidates       : min=%s max=%s avg=%s mid=%s p70=%s p80=%s p90=%s%n",
                    candidates._1(), candidates._2(), candidates._3(), candidates._4(),
                    candidates._5(), candidates._6(), candidates._7());
            System.out.printf("Size Stats        : min=%s max=%s avg=%s mid=%s p70=%s p80=%s p90=%s%n",
                    size._1(), size._2(), size._3(), size._4(), size._5(), size._6(), size._7());
            if (checkMode) {
                System.out.println("Brute-force Check Summary:");
                System.out.printf("  Pass Count      : %d / %d%n", bruteForcePassCount, processedQueries);
                System.out.printf("  Warn Count      : %d / %d%n", warningCount, processedQueries);
            }
            System.out.printf("Result File       : %s%n", resultPath);
            System.out.println("================================================================================");
        }

        System.out.printf("Similarity query finished. distance=%s, threshold=%s, queryLimit=%d, checkMode=%s%n",
                TrajectorySimilarity.functionName(func), threshold, queryLimit, checkMode);
    }

    private static String previewText(String text, int maxLength) {
        if (text == null) {
            return "";
        }
        String normalized = text.replace('\n', ' ').replace('\r', ' ').trim();
        if (normalized.length() <= maxLength) {
            return normalized;
        }
        return normalized.substring(0, maxLength) + "...";
    }

    private static String csv(String value) {
        if (value == null) {
            return "\"\"";
        }
        return "\"" + value.replace("\"", "\"\"") + "\"";
    }

    private static ValidationSummary validateAgainstBruteForce(
            List<TrajectorySimilarityQuerySupport.ScoredTrajectory> indexedResult,
            List<TrajectorySimilarityQuerySupport.ScoredTrajectory> bruteForceResult) {
        java.util.Map<String, Double> indexedMap = toDistanceMap(indexedResult);
        java.util.Map<String, Double> bruteForceMap = toDistanceMap(bruteForceResult);
        int missingCount = 0;
        int extraCount = 0;
        int distanceMismatchCount = 0;
        StringBuilder message = new StringBuilder();

        for (java.util.Map.Entry<String, Double> entry : bruteForceMap.entrySet()) {
            Double indexedDistance = indexedMap.get(entry.getKey());
            if (indexedDistance == null) {
                missingCount++;
                appendMessage(message, "missing=" + entry.getKey());
                continue;
            }
            if (Math.abs(indexedDistance - entry.getValue()) > DISTANCE_COMPARE_EPSILON) {
                distanceMismatchCount++;
                appendMessage(message, String.format(
                        Locale.ROOT,
                        "distanceMismatch=%s indexed=%.6f brute=%.6f",
                        entry.getKey(),
                        indexedDistance,
                        entry.getValue()
                ));
            }
        }

        for (String key : indexedMap.keySet()) {
            if (!bruteForceMap.containsKey(key)) {
                extraCount++;
                appendMessage(message, "extra=" + key);
            }
        }

        if (message.length() == 0) {
            appendMessage(message, "indexed result matches brute-force result");
        }

        boolean passed = missingCount == 0 && extraCount == 0 && distanceMismatchCount == 0;
        return new ValidationSummary(
                passed,
                indexedResult.size(),
                bruteForceResult.size(),
                missingCount,
                extraCount,
                distanceMismatchCount,
                message.toString()
        );
    }

    private static java.util.Map<String, Double> toDistanceMap(
            List<TrajectorySimilarityQuerySupport.ScoredTrajectory> result) {
        java.util.Map<String, Double> map = new java.util.LinkedHashMap<>();
        for (TrajectorySimilarityQuerySupport.ScoredTrajectory scored : result) {
            map.put(scored.trajectory.getOid() + "#" + scored.trajectory.getTid(), scored.distance);
        }
        return map;
    }

    private static void appendMessage(StringBuilder builder, String part) {
        if (part == null || part.isEmpty()) {
            return;
        }
        if (builder.length() > 0) {
            builder.append("; ");
        }
        builder.append(part);
    }

    public List<TrajectorySimilarityQuerySupport.ScoredTrajectory> simQuery(Trajectory trajectory,
                                                                            String tableName,
                                                                            double threshold,
                                                                            int func) throws IOException {
        try (TrajectorySimilarityQuerySupport support = new TrajectorySimilarityQuerySupport()) {
            return support.similarityQuery(tableName, trajectory, threshold, func);
        }
    }

    @Override
    public void close() {
    }

    private static final class ValidationSummary {
        private final boolean passed;
        private final int indexedResultSize;
        private final int bruteForceResultSize;
        private final int missingCount;
        private final int extraCount;
        private final int distanceMismatchCount;
        private final String message;

        private ValidationSummary(boolean passed,
                                  int indexedResultSize,
                                  int bruteForceResultSize,
                                  int missingCount,
                                  int extraCount,
                                  int distanceMismatchCount,
                                  String message) {
            this.passed = passed;
            this.indexedResultSize = indexedResultSize;
            this.bruteForceResultSize = bruteForceResultSize;
            this.missingCount = missingCount;
            this.extraCount = extraCount;
            this.distanceMismatchCount = distanceMismatchCount;
            this.message = message;
        }
    }
}
