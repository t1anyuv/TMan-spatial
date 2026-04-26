package experiments.standalone.query;

import entity.Trajectory;
import org.apache.hadoop.hbase.util.SortedList;
import scala.Tuple7;
import similarity.TrajectorySimilarity;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Locale;

public class TopKQuery implements AutoCloseable {
    public static void main(String[] args) throws IOException {
        if (args.length < 5) {
            System.err.println("Usage: TopKQuery <table> <query_traj_file> <k> <distance_func> <result_csv> [initial_threshold]");
            System.err.println("distance_func: 0=Frechet, 1=Hausdorff, 2=DTW");
            return;
        }

        String tableName = args[0];
        String queryTrajectoryFile = args[1];
        int k = Integer.parseInt(args[2]);
        int func = Integer.parseInt(args[3]);
        String resultPath = args[4];
        double initialThreshold = args.length >= 6 ? Double.parseDouble(args[5]) : 0.0d;

        SortedList<Long> timeStatistic = new SortedList<>(Long::compare);
        SortedList<Long> sizeStatistic = new SortedList<>(Long::compare);
        SortedList<Long> rowKeyRangeStatistic = new SortedList<>(Long::compare);
        SortedList<Long> candidatesStatistic = new SortedList<>(Long::compare);

        try (TrajectorySimilarityQuerySupport support = new TrajectorySimilarityQuerySupport();
             BufferedReader reader = new BufferedReader(new FileReader(queryTrajectoryFile));
             FileWriter writer = new FileWriter(resultPath)) {
            System.out.println("================================================================================");
            System.out.println("Top-K Query Started");
            System.out.println("================================================================================");
            System.out.printf("Table            : %s%n", tableName);
            System.out.printf("Query File       : %s%n", queryTrajectoryFile);
            System.out.printf("K                : %d%n", k);
            System.out.printf("Distance Func    : %s%n", TrajectorySimilarity.functionName(func));
            System.out.printf("Initial Threshold: %s%n", initialThreshold);
            System.out.printf("Result File      : %s%n", resultPath);
            System.out.println("================================================================================");

            String line;
            int queryIndex = 0;
            while ((line = reader.readLine()) != null) {
                if (line.trim().isEmpty()) {
                    continue;
                }
                queryIndex++;
                Trajectory query = TrajectorySimilarityQuerySupport.parseQueryTrajectory(line.trim());
                System.out.println("--------------------------------------------------------------------------------");
                System.out.printf("Query %d%n", queryIndex);
                System.out.printf("  OID/TID      : %s / %s%n", query.getOid(), query.getTid());
                System.out.printf("  Point Count  : %d%n", query.getNumGeometries());
                System.out.printf("  Query WKT    : %s%n", previewText(query.toText(), 160));
                long start = System.currentTimeMillis();
                TrajectorySimilarityQuerySupport.TopKQueryResult queryResult =
                        support.topKQueryWithStats(tableName, query, k, func, initialThreshold);
                long elapsed = System.currentTimeMillis() - start;
                List<TrajectorySimilarityQuerySupport.ScoredTrajectory> result = queryResult.getResults();
                timeStatistic.add(elapsed);
                sizeStatistic.add((long) result.size());
                rowKeyRangeStatistic.add(queryResult.getTotalRowKeyRangeCount());
                candidatesStatistic.add(queryResult.getTotalCandidateCount());
                System.out.printf("  Query Time   : %d ms%n", elapsed);
                System.out.printf("  Iterations   : %d%n", queryResult.getIterations());
                System.out.printf("  Row Ranges   : %d%n", queryResult.getTotalRowKeyRangeCount());
                System.out.printf("  Candidates   : %d%n", queryResult.getTotalCandidateCount());
                System.out.printf("  Result Count : %d%n", result.size());
                if (result.isEmpty()) {
                    System.out.println("  Result Preview: no result");
                } else {
                    System.out.println("  Result Preview:");
                    for (int i = 0; i < Math.min(5, result.size()); i++) {
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
            System.out.println("Top-K Query Summary");
            System.out.println("================================================================================");
            System.out.printf("Time Stats (ms) : min=%s max=%s avg=%s mid=%s p70=%s p80=%s p90=%s%n",
                    time._1(), time._2(), time._3(), time._4(), time._5(), time._6(), time._7());
            System.out.printf("Row Ranges     : min=%s max=%s avg=%s mid=%s p70=%s p80=%s p90=%s%n",
                    rowKeyRanges._1(), rowKeyRanges._2(), rowKeyRanges._3(), rowKeyRanges._4(),
                    rowKeyRanges._5(), rowKeyRanges._6(), rowKeyRanges._7());
            System.out.printf("Candidates     : min=%s max=%s avg=%s mid=%s p70=%s p80=%s p90=%s%n",
                    candidates._1(), candidates._2(), candidates._3(), candidates._4(),
                    candidates._5(), candidates._6(), candidates._7());
            System.out.printf("Final Size     : min=%s max=%s avg=%s mid=%s p70=%s p80=%s p90=%s%n",
                    size._1(), size._2(), size._3(), size._4(), size._5(), size._6(), size._7());
            System.out.printf("Result File    : %s%n", resultPath);
            System.out.println("================================================================================");
        }

        System.out.printf("Top-k query finished. k=%d, distance=%s, initialThreshold=%s%n",
                k, TrajectorySimilarity.functionName(func), initialThreshold);
    }

    public List<TrajectorySimilarityQuerySupport.ScoredTrajectory> topKQuery(Trajectory trajectory,
                                                                             String tableName,
                                                                             int k,
                                                                             int func,
                                                                             double initialThreshold) throws IOException {
        try (TrajectorySimilarityQuerySupport support = new TrajectorySimilarityQuerySupport()) {
            return support.topKQuery(tableName, trajectory, k, func, initialThreshold);
        }
    }

    @Override
    public void close() {
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
}
