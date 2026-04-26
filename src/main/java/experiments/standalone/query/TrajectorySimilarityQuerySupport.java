package experiments.standalone.query;

import config.TableConfig;
import entity.Trajectory;
import filter.TrajectorySimilarityFilter;
import index.LETILocSIndex;
import index.LocSIndex;
import index.XZStarSFC;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.MultiRowRangeFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.MultiPoint;
import org.locationtech.jts.geom.PrecisionModel;
import org.locationtech.sfcurve.IndexRange;
import preprocess.compress.IIntegerCompress;
import query.QueryPlanner;
import redis.clients.jedis.Jedis;
import similarity.TrajectorySimilarity;
import utils.QueryUtils;
import utils.RedisPoolManager;
import utils.TrajectoryParser;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;

import static client.Constants.DEFAULT_CF;
import static client.Constants.END_POINT;
import static client.Constants.GEOM_X;
import static client.Constants.GEOM_Y;
import static client.Constants.GEOM_Z;
import static client.Constants.META_TABLE;
import static client.Constants.O_ID;
import static client.Constants.PIVOT_MBR;
import static client.Constants.PIVOT_POINT;
import static client.Constants.START_POINT;
import static client.Constants.T_ID;

public final class TrajectorySimilarityQuerySupport implements Closeable {
    private static final GeometryFactory GEOMETRY_FACTORY = new GeometryFactory();
    private static final double COORDINATE_SCALE = 1_000_000.0d;
    private static final int MAX_KNN_ITERATOR = 100;
    private final QueryUtils queryUtils;
    private final Connection connection;
    public TrajectorySimilarityQuerySupport() throws IOException {
        this.queryUtils = new QueryUtils();
        Configuration configuration = HBaseConfiguration.create();
        this.connection = ConnectionFactory.createConnection(configuration);
    }

    private static XZStarSFC createXZStar(TableConfig config) {
        if (config.getEnvelope() != null) {
            return XZStarSFC.apply(
                    (short) config.getResolution(),
                    new scala.Tuple2<>(config.getEnvelope().getXMin(), config.getEnvelope().getXMax()),
                    new scala.Tuple2<>(config.getEnvelope().getYMin(), config.getEnvelope().getYMax()),
                    config.getBeta()
            );
        }
        return XZStarSFC.apply(
                (short) config.getResolution(),
                new scala.Tuple2<>(-180.0d, 180.0d),
                new scala.Tuple2<>(-90.0d, 90.0d),
                config.getBeta()
        );
    }

    private static double defaultInitialThreshold(Trajectory trajectory, TableConfig config) {
        double base = Math.max(trajectory.getXLength(), trajectory.getYLength());
        if (base > 0.0d) {
            return base;
        }
        if (config.getEnvelope() != null) {
            double width = config.getEnvelope().getXMax() - config.getEnvelope().getXMin();
            double height = config.getEnvelope().getYMax() - config.getEnvelope().getYMin();
            return Math.max(width, height) / Math.max(4.0d, config.getResolution());
        }
        return 0.01d;
    }

    private static Trajectory decodeTrajectory(Result result, TableConfig config) {
        byte[] oidBytes = result.getValue(Bytes.toBytes(DEFAULT_CF), Bytes.toBytes(O_ID));
        byte[] tidBytes = result.getValue(Bytes.toBytes(DEFAULT_CF), Bytes.toBytes(T_ID));
        byte[] xBytes = result.getValue(Bytes.toBytes(DEFAULT_CF), Bytes.toBytes(GEOM_X));
        byte[] yBytes = result.getValue(Bytes.toBytes(DEFAULT_CF), Bytes.toBytes(GEOM_Y));
        byte[] zBytes = result.getValue(Bytes.toBytes(DEFAULT_CF), Bytes.toBytes(GEOM_Z));
        if (oidBytes == null || tidBytes == null || xBytes == null || yBytes == null || zBytes == null) {
            return null;
        }

        String oid;
        try {
            oid = String.valueOf(Bytes.toLong(oidBytes));
        } catch (Exception e) {
            oid = Bytes.toString(oidBytes);
        }
        String tid = Bytes.toString(tidBytes);

        IIntegerCompress compressor = IIntegerCompress.getIntegerCompress(config.getCompressType().name());
        int[] xs = compressor.decoding(xBytes);
        int[] ys = compressor.decoding(yBytes);
        int[] zs = compressor.decoding(zBytes);
        int size = Math.min(xs.length, Math.min(ys.length, zs.length));
        if (size == 0) {
            return null;
        }

        Coordinate[] coordinates = new Coordinate[size];
        for (int i = 0; i < size; i++) {
            Coordinate coordinate = new Coordinate(xs[i] / COORDINATE_SCALE, ys[i] / COORDINATE_SCALE);
            coordinate.setZ(zs[i]);
            coordinates[i] = coordinate;
        }
        MultiPoint multiPoint = GEOMETRY_FACTORY.createMultiPointFromCoords(coordinates);
        return new Trajectory(oid, tid, multiPoint);
    }

    public static Trajectory parseQueryTrajectory(String line) {
        TrajectoryParser.ParsedTrajectory parsed = TrajectoryParser.parse(line);
        return new Trajectory(parsed.oid, parsed.tid, parsed.jtsGeo);
    }

    static String trajectoryKey(Trajectory trajectory) {
        return trajectory.getOid() + "#" + trajectory.getTid();
    }

    private static boolean isSameTrajectory(Trajectory left, Trajectory right) {
        if (left == null || right == null) {
            return false;
        }
        return safeEquals(left.getOid(), right.getOid()) && safeEquals(left.getTid(), right.getTid());
    }

    private static boolean safeEquals(String left, String right) {
        if (left == null) {
            return right == null;
        }
        return left.equals(right);
    }

    public List<ScoredTrajectory> similarityQuery(String tableName, Trajectory queryTrajectory, double threshold, int func) throws IOException {
        return similarityQueryWithStats(tableName, queryTrajectory, threshold, func).results;
    }

    public SimilarityQueryResult similarityQueryWithStats(String tableName,
                                                          Trajectory queryTrajectory,
                                                          double threshold,
                                                          int func) throws IOException {
        TableConfig config = queryUtils.getTableConfig(tableName + META_TABLE);
        List<IndexRange> ranges = collectCandidateRanges(tableName, config, queryTrajectory, threshold);
        List<MultiRowRangeFilter.RowRange> rowRanges = QueryPlanner.rangesToRowkey(ranges, config);
        long candidateCount = countCandidates(tableName, rowRanges, queryTrajectory);
        List<ScoredTrajectory> results = filterAndScore(tableName, config, queryTrajectory, ranges, func, threshold, true);
        return new SimilarityQueryResult(results, rowRanges.size(), candidateCount);
    }

    public List<ScoredTrajectory> bruteForceSimilarityQuery(String tableName,
                                                            Trajectory queryTrajectory,
                                                            double threshold,
                                                            int func) throws IOException {
        TableConfig config = queryUtils.getTableConfig(tableName + META_TABLE);
        Map<String, ScoredTrajectory> bestSeen = new LinkedHashMap<>();

        try (Table table = connection.getTable(TableName.valueOf(tableName))) {
            Scan scan = buildBruteForceScan();
            try (ResultScanner scanner = table.getScanner(scan)) {
                for (Result result : scanner) {
                    Trajectory candidate = decodeTrajectory(result, config);
                    if (candidate == null) {
                        continue;
                    }
                    if (isSameTrajectory(queryTrajectory, candidate)) {
                        continue;
                    }
                    double distance = TrajectorySimilarity.calculateDistance(queryTrajectory, candidate, func);
                    if (distance > threshold) {
                        continue;
                    }
                    String key = trajectoryKey(candidate);
                    ScoredTrajectory previous = bestSeen.get(key);
                    if (previous == null || distance < previous.distance) {
                        bestSeen.put(key, new ScoredTrajectory(candidate, distance));
                    }
                }
            }
        }

        List<ScoredTrajectory> scored = new ArrayList<>(bestSeen.values());
        scored.sort(Comparator.comparingDouble(v -> v.distance));
        return scored;
    }

    public List<ScoredTrajectory> topKQuery(String tableName, Trajectory queryTrajectory, int k, int func, double initialThreshold) throws IOException {
        return topKQueryWithStats(tableName, queryTrajectory, k, func, initialThreshold).results;
    }

    public TopKQueryResult topKQueryWithStats(String tableName,
                                              Trajectory queryTrajectory,
                                              int k,
                                              int func,
                                              double initialThreshold) throws IOException {
        if (k <= 0) {
            return new TopKQueryResult(Collections.emptyList(), 0L, 0L, 0);
        }

        TableConfig config = queryUtils.getTableConfig(tableName + META_TABLE);
        double interval = initialThreshold > 0.0d ? initialThreshold : defaultInitialThreshold(queryTrajectory, config);
        TopKState state = new TopKState(k);
        long totalRowKeyRangeCount = 0L;
        long totalCandidateCount = 0L;
        int executedIterations = 0;
        for (int iter = 0; iter <= MAX_KNN_ITERATOR; iter++) {
            double threshold = interval * iter;
            List<IndexRange> ranges = collectCandidateRanges(tableName, config, queryTrajectory, threshold);
            List<MultiRowRangeFilter.RowRange> rowRanges = QueryPlanner.rangesToRowkey(ranges, config);
            totalRowKeyRangeCount += rowRanges.size();
            totalCandidateCount += countCandidates(tableName, rowRanges, queryTrajectory);
            executedIterations++;
            double pruneThreshold = state.currentThreshold();
            collectTopKBatch(
                    tableName,
                    config,
                    queryTrajectory,
                    rowRanges,
                    func,
                    pruneThreshold,
                    state.hasEnoughCandidates(),
                    state
            );

            double kthDistance = state.currentThreshold();
            if (!Double.isNaN(kthDistance) && kthDistance <= threshold) {
                return new TopKQueryResult(state.ranked(), totalRowKeyRangeCount, totalCandidateCount, executedIterations);
            }
        }

        return new TopKQueryResult(state.ranked(), totalRowKeyRangeCount, totalCandidateCount, executedIterations);
    }

    private List<IndexRange> collectCandidateRanges(String tableName,
                                                    TableConfig config,
                                                    Trajectory queryTrajectory,
                                                    double threshold) {
        switch (config.getSpatialIndexKind()) {
            case XZPlus:
                throw new UnsupportedOperationException(
                        "XZPlus similarity/top-k query is not supported yet: the table is encoded with XZLocSIndex, " +
                                "but no dedicated XZPlus candidate-range generator exists for similarity search."
                );
            case XZ_STAR:
                return collectXZStarRanges(config, queryTrajectory, threshold);
            case LETI:
                return collectLetiRanges(tableName, config, queryTrajectory, threshold);
            case TShape:
            default:
                return collectLocSRanges(tableName, config, queryTrajectory, threshold);
        }
    }

    private List<IndexRange> collectLocSRanges(String tableName,
                                               TableConfig config,
                                               Trajectory queryTrajectory,
                                               double threshold) {
        LocSIndex locSIndex;
        if (config.getEnvelope() != null) {
            locSIndex = LocSIndex.apply(
                    (short) config.getResolution(),
                    new scala.Tuple2<>(config.getEnvelope().getXMin(), config.getEnvelope().getXMax()),
                    new scala.Tuple2<>(config.getEnvelope().getYMin(), config.getEnvelope().getYMax()),
                    config.getAlpha(),
                    config.getBeta()
            );
        } else {
            locSIndex = LocSIndex.apply((short) config.getResolution(), config.getAlpha(), config.getBeta());
        }

        try (Jedis jedis = RedisPoolManager.getResource(config.getRedisHost())) {
            return locSIndex.similarityRanges(queryTrajectory, threshold, jedis, tableName, config.isTspEncoding());
        }
    }

    private List<IndexRange> collectLetiRanges(String tableName,
                                               TableConfig config,
                                               Trajectory queryTrajectory,
                                               double threshold) {
        LETILocSIndex letiIndex;
        if (config.getEnvelope() != null) {
            letiIndex = LETILocSIndex.apply(
                    tableName,
                    (short) config.getResolution(),
                    new scala.Tuple2<>(config.getEnvelope().getXMin(), config.getEnvelope().getXMax()),
                    new scala.Tuple2<>(config.getEnvelope().getYMin(), config.getEnvelope().getYMax()),
                    config.getAlpha(),
                    config.getBeta(),
                    config.isAdaptivePartition(),
                    config.getOrderDefinitionPath()
            );
        } else {
            letiIndex = LETILocSIndex.apply(
                    tableName,
                    (short) config.getResolution(),
                    config.getAlpha(),
                    config.getBeta(),
                    config.isAdaptivePartition(),
                    config.getOrderDefinitionPath()
            );
        }

        try (Jedis jedis = RedisPoolManager.getResource(config.getRedisHost())) {
            return letiIndex.similarityRanges(queryTrajectory, threshold, jedis, tableName);
        }
    }

    private List<IndexRange> collectXZStarRanges(TableConfig config,
                                                 Trajectory queryTrajectory,
                                                 double threshold) {
        XZStarSFC sfc = createXZStar(config);
        index.ElementKNN root = new index.ElementKNN(
                config.getEnvelope() != null ? config.getEnvelope().getXMin() : -180.0d,
                config.getEnvelope() != null ? config.getEnvelope().getYMin() : -90.0d,
                config.getEnvelope() != null ? config.getEnvelope().getXMax() : 180.0d,
                config.getEnvelope() != null ? config.getEnvelope().getYMax() : 90.0d,
                0,
                config.getResolution(),
                new PrecisionModel(),
                0L
        );
        return sfc.rangesForKnn(queryTrajectory, threshold, root);
    }

    private List<ScoredTrajectory> filterAndScore(String tableName,
                                                  TableConfig config,
                                                  Trajectory queryTrajectory,
                                                  List<IndexRange> ranges,
                                                  int func,
                                                  double threshold,
                                                  boolean applyThreshold) throws IOException {
        if (ranges == null || ranges.isEmpty()) {
            return Collections.emptyList();
        }

        List<MultiRowRangeFilter.RowRange> rowRanges = QueryPlanner.rangesToRowkey(ranges, config);
        if (rowRanges.isEmpty()) {
            return Collections.emptyList();
        }

        Map<String, ScoredTrajectory> bestSeen = new LinkedHashMap<>();

        try (Table table = connection.getTable(TableName.valueOf(tableName))) {
            Scan scan = buildSimilarityScan(rowRanges, queryTrajectory, config, threshold, func, applyThreshold);

            try (ResultScanner scanner = table.getScanner(scan)) {
                for (Result result : scanner) {
                    Trajectory candidate = decodeTrajectory(result, config);
                    if (candidate == null) {
                        continue;
                    }
                    if (isSameTrajectory(queryTrajectory, candidate)) {
                        continue;
                    }
                    double distance = TrajectorySimilarity.calculateDistance(queryTrajectory, candidate, func);
                    if (applyThreshold && distance > threshold) {
                        continue;
                    }

                    String key = trajectoryKey(candidate);
                    ScoredTrajectory previous = bestSeen.get(key);
                    if (previous == null || distance < previous.distance) {
                        bestSeen.put(key, new ScoredTrajectory(candidate, distance));
                    }
                }
            }
        }

        List<ScoredTrajectory> scored = new ArrayList<>(bestSeen.values());
        scored.sort(Comparator.comparingDouble(v -> v.distance));
        return scored;
    }

    private void collectTopKBatch(String tableName,
                                  TableConfig config,
                                  Trajectory queryTrajectory,
                                  List<MultiRowRangeFilter.RowRange> rowRanges,
                                  int func,
                                  double threshold,
                                  boolean applyDistanceThreshold,
                                  TopKState state) throws IOException {
        if (rowRanges.isEmpty()) {
            return;
        }

        try (Table table = connection.getTable(TableName.valueOf(tableName))) {
            Scan scan = buildSimilarityScan(rowRanges, queryTrajectory, config, threshold, func, applyDistanceThreshold);

            try (ResultScanner scanner = table.getScanner(scan)) {
                for (Result result : scanner) {
                    Trajectory candidate = decodeTrajectory(result, config);
                    if (candidate == null) {
                        continue;
                    }
                    if (isSameTrajectory(queryTrajectory, candidate)) {
                        continue;
                    }
                    double distance = TrajectorySimilarity.calculateDistance(queryTrajectory, candidate, func);
                    if (!Double.isNaN(threshold) && distance > threshold) {
                        continue;
                    }
                    state.offer(new ScoredTrajectory(candidate, distance));
                }
            }
        }
    }

    private Scan buildSimilarityScan(List<MultiRowRangeFilter.RowRange> rowRanges,
                                     Trajectory queryTrajectory,
                                     TableConfig config,
                                     double threshold,
                                     int func,
                                     boolean applyDistanceThreshold) {
        Scan scan = new Scan();
        scan.setCaching(1000);
        byte[] cf = Bytes.toBytes(DEFAULT_CF);
        scan.addColumn(cf, Bytes.toBytes(O_ID));
        scan.addColumn(cf, Bytes.toBytes(T_ID));
        scan.addColumn(cf, Bytes.toBytes(GEOM_X));
        scan.addColumn(cf, Bytes.toBytes(GEOM_Y));
        scan.addColumn(cf, Bytes.toBytes(GEOM_Z));
        scan.addColumn(cf, Bytes.toBytes(START_POINT));
        scan.addColumn(cf, Bytes.toBytes(END_POINT));
        scan.addColumn(cf, Bytes.toBytes(PIVOT_POINT));
        scan.addColumn(cf, Bytes.toBytes(PIVOT_MBR));

        FilterList filterList = new FilterList();
        filterList.addFilter(new MultiRowRangeFilter(rowRanges));
        filterList.addFilter(new TrajectorySimilarityFilter(
                queryTrajectory.toString(),
                config.getCompressType().name(),
                threshold,
                func,
                applyDistanceThreshold
        ));
        scan.setFilter(filterList);
        return scan;
    }

    private Scan buildBruteForceScan() {
        Scan scan = new Scan();
        scan.setCaching(1000);
        byte[] cf = Bytes.toBytes(DEFAULT_CF);
        scan.addColumn(cf, Bytes.toBytes(O_ID));
        scan.addColumn(cf, Bytes.toBytes(T_ID));
        scan.addColumn(cf, Bytes.toBytes(GEOM_X));
        scan.addColumn(cf, Bytes.toBytes(GEOM_Y));
        scan.addColumn(cf, Bytes.toBytes(GEOM_Z));
        return scan;
    }

    private long countCandidates(String tableName,
                                 List<MultiRowRangeFilter.RowRange> rowRanges,
                                 Trajectory queryTrajectory) throws IOException {
        if (rowRanges == null || rowRanges.isEmpty()) {
            return 0L;
        }

        long candidateCount = 0L;
        try (Table table = connection.getTable(TableName.valueOf(tableName))) {
            Scan scan = new Scan();
            scan.setCaching(1000);
            byte[] cf = Bytes.toBytes(DEFAULT_CF);
            scan.addColumn(cf, Bytes.toBytes(O_ID));
            scan.addColumn(cf, Bytes.toBytes(T_ID));
            scan.setFilter(new MultiRowRangeFilter(rowRanges));

            try (ResultScanner scanner = table.getScanner(scan)) {
                for (Result result : scanner) {
                    String oid = readOid(result.getValue(cf, Bytes.toBytes(O_ID)));
                    String tid = Bytes.toString(result.getValue(cf, Bytes.toBytes(T_ID)));
                    if (safeEquals(queryTrajectory.getOid(), oid) && safeEquals(queryTrajectory.getTid(), tid)) {
                        continue;
                    }
                    candidateCount++;
                }
            }
        }
        return candidateCount;
    }

    private String readOid(byte[] oidBytes) {
        if (oidBytes == null) {
            return "";
        }
        try {
            return String.valueOf(Bytes.toLong(oidBytes));
        } catch (Exception e) {
            return Bytes.toString(oidBytes);
        }
    }

    @Override
    public void close() throws IOException {
        connection.close();
    }

    public static final class ScoredTrajectory {
        public final Trajectory trajectory;
        public final double distance;

        public ScoredTrajectory(Trajectory trajectory, double distance) {
            this.trajectory = trajectory;
            this.distance = distance;
        }
    }

    public static final class SimilarityQueryResult {
        private final List<ScoredTrajectory> results;
        private final int rowKeyRangeCount;
        private final long candidateCount;

        private SimilarityQueryResult(List<ScoredTrajectory> results,
                                      int rowKeyRangeCount,
                                      long candidateCount) {
            this.results = results;
            this.rowKeyRangeCount = rowKeyRangeCount;
            this.candidateCount = candidateCount;
        }

        public List<ScoredTrajectory> getResults() {
            return results;
        }

        public int getRowKeyRangeCount() {
            return rowKeyRangeCount;
        }

        public long getCandidateCount() {
            return candidateCount;
        }
    }

    public static final class TopKQueryResult {
        private final List<ScoredTrajectory> results;
        private final long totalRowKeyRangeCount;
        private final long totalCandidateCount;
        private final int iterations;

        private TopKQueryResult(List<ScoredTrajectory> results,
                                long totalRowKeyRangeCount,
                                long totalCandidateCount,
                                int iterations) {
            this.results = results;
            this.totalRowKeyRangeCount = totalRowKeyRangeCount;
            this.totalCandidateCount = totalCandidateCount;
            this.iterations = iterations;
        }

        public List<ScoredTrajectory> getResults() {
            return results;
        }

        public long getTotalRowKeyRangeCount() {
            return totalRowKeyRangeCount;
        }

        public long getTotalCandidateCount() {
            return totalCandidateCount;
        }

        public int getIterations() {
            return iterations;
        }
    }

    private static final class TopKState {
        private final int k;
        private final Map<String, ScoredTrajectory> bestById = new LinkedHashMap<>();
        private final PriorityQueue<ScoredTrajectory> maxHeap =
                new PriorityQueue<>(Comparator.comparingDouble((ScoredTrajectory v) -> v.distance).reversed());

        private TopKState(int k) {
            this.k = k;
        }

        boolean hasEnoughCandidates() {
            return bestById.size() >= k;
        }

        void offer(ScoredTrajectory scored) {
            if (scored == null || scored.trajectory == null) {
                return;
            }
            String key = trajectoryKey(scored.trajectory);
            ScoredTrajectory previous = bestById.get(key);
            if (previous != null && previous.distance <= scored.distance) {
                return;
            }
            bestById.put(key, scored);
            maxHeap.offer(scored);
            trimToK();
        }

        double currentThreshold() {
            if (bestById.size() < k) {
                return Double.MAX_VALUE;
            }
            removeStaleHead();
            ScoredTrajectory head = maxHeap.peek();
            return head == null ? Double.MAX_VALUE : head.distance;
        }

        List<ScoredTrajectory> ranked() {
            List<ScoredTrajectory> ranked = new ArrayList<>(bestById.values());
            ranked.sort(Comparator.comparingDouble(v -> v.distance));
            return ranked;
        }

        private void trimToK() {
            removeStaleHead();
            while (bestById.size() > k) {
                ScoredTrajectory removed = maxHeap.poll();
                if (removed == null) {
                    break;
                }
                String key = trajectoryKey(removed.trajectory);
                ScoredTrajectory current = bestById.get(key);
                if (current == removed) {
                    bestById.remove(key);
                }
                removeStaleHead();
            }
        }

        private void removeStaleHead() {
            while (!maxHeap.isEmpty()) {
                ScoredTrajectory head = maxHeap.peek();
                if (head == null || head.trajectory == null) {
                    maxHeap.poll();
                    continue;
                }
                ScoredTrajectory current = bestById.get(trajectoryKey(head.trajectory));
                if (current != head) {
                    maxHeap.poll();
                    continue;
                }
                break;
            }
        }
    }
}
