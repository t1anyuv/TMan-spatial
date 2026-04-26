package utils;

import org.json.JSONArray;
import org.json.JSONObject;
import org.locationtech.jts.geom.Envelope;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Random;

/**
 * BMTree config builder.
 * <p>
 * The learned variant follows the original Learned-BMTree spirit:
 * it learns from real data samples plus real query workloads, then greedily
 * picks the split bit that minimizes the average query scan range on the sampled data.
 * <p>
 * In this project the raw data are trajectories rather than points, so each sampled
 * trajectory is represented by:
 * - its envelope, used for query-hit testing
 * - its envelope lower-left corner, used as the ordering point on the learned curve
 */
public final class BMTreeConfigLearner {

    public static final String DEFAULT_TDRIVE_OUTPUT = "src/main/resources/bmtree/tdrive/tdrive_bmtree.txt";
    public static final String DEFAULT_CDTAXI_OUTPUT = "src/main/resources/bmtree/cdtaxi/cdtaxi_bmtree.txt";

    public static final int DEFAULT_TRAJECTORY_SAMPLE_LIMIT = 4_000;
    public static final int DEFAULT_QUERY_SAMPLE_LIMIT = 300;
    public static final int DEFAULT_SAMPLE_LIMIT = DEFAULT_QUERY_SAMPLE_LIMIT;
    public static final int DEFAULT_PAGE_SIZE = 64;
    public static final int DEFAULT_MIN_NODE_SIZE = 32;
    public static final long DEFAULT_SEED = 20260421L;

    private BMTreeConfigLearner() {
    }

    public static String ensureConfigExists(String configPath, int[] bitLength, int maxDepth) throws IOException {
        if (configPath == null || configPath.trim().isEmpty()) {
            throw new IllegalArgumentException("BMTree config path must not be empty");
        }
        Path directPath = Paths.get(configPath);
        if (Files.exists(directPath)) {
            return directPath.toString();
        }
        String resourcePath = normalizeResourcePath(configPath);
        if (BMTreeConfigLearner.class.getClassLoader().getResource(resourcePath) != null) {
            Path outputPath = resolveOutputPath(configPath);
            mirrorResourceToFile(resourcePath, outputPath);
            return outputPath.toString();
        }
        Path outputPath = resolveOutputPath(configPath);
        buildZOrder(outputPath.toString(), bitLength, maxDepth);
        return outputPath.toString();
    }

    public static String ensureLearnedConfigExists(String configPath,
                                                   int[] bitLength,
                                                   int maxDepth,
                                                   String sampleDataPath,
                                                   String queryWorkloadPath,
                                                   int trajectorySampleLimit,
                                                   int querySampleLimit,
                                                   int pageSize,
                                                   int minNodeSize,
                                                   long seed) throws IOException {
        if (sampleDataPath == null || sampleDataPath.trim().isEmpty()
                || queryWorkloadPath == null || queryWorkloadPath.trim().isEmpty()) {
            return ensureConfigExists(configPath, bitLength, maxDepth);
        }
        if (configPath == null || configPath.trim().isEmpty()) {
            throw new IllegalArgumentException("BMTree config path must not be empty");
        }
        Path directPath = Paths.get(configPath);
        if (Files.exists(directPath)) {
            return directPath.toString();
        }
        String resourcePath = normalizeResourcePath(configPath);
        if (BMTreeConfigLearner.class.getClassLoader().getResource(resourcePath) != null) {
            Path outputPath = resolveOutputPath(configPath);
            mirrorResourceToFile(resourcePath, outputPath);
            return outputPath.toString();
        }

        Path outputPath = resolveOutputPath(configPath);
        try {
            buildLearned(
                    outputPath.toString(),
                    bitLength,
                    maxDepth,
                    sampleDataPath,
                    queryWorkloadPath,
                    trajectorySampleLimit,
                    querySampleLimit,
                    pageSize,
                    minNodeSize,
                    seed
            );
        } catch (RuntimeException e) {
            System.err.println("[BMTreeConfigLearner] Learned build failed, fallback to z-order: " + e.getMessage());
            buildZOrder(outputPath.toString(), bitLength, maxDepth);
        }
        return outputPath.toString();
    }

    public static void buildZOrder(String outputPath, int[] bitLength, int maxDepth) throws IOException {
        buildInternal(outputPath, bitLength, maxDepth, null);
    }

    public static void buildLearned(String outputPath,
                                    int[] bitLength,
                                    int maxDepth,
                                    String sampleDataPath,
                                    String queryWorkloadPath,
                                    int trajectorySampleLimit,
                                    int querySampleLimit,
                                    int pageSize,
                                    int minNodeSize,
                                    long seed) throws IOException {
        validateBitLength(bitLength);

        Random random = new Random(seed);
        List<RawTrajectorySample> rawSamples = loadTrajectorySamples(sampleDataPath, trajectorySampleLimit, random);
        if (rawSamples.isEmpty()) {
            throw new IllegalArgumentException("No valid trajectory samples found in: " + sampleDataPath);
        }

        List<QueryWindow> queries = loadWorkloadSamples(queryWorkloadPath, querySampleLimit);
        if (queries.isEmpty()) {
            throw new IllegalArgumentException("No valid query windows found in: " + queryWorkloadPath);
        }

        Bounds bounds = computeBounds(rawSamples, queries);
        List<TrajectorySample> samples = normalizeSamples(rawSamples, bitLength, bounds);
        List<NodeRecord> learnedTree = learnTree(bitLength, maxDepth, samples, queries, pageSize, minNodeSize);

        System.out.println("BMTree sampled trajectories: " + samples.size());
        System.out.println("BMTree sampled queries     : " + queries.size());
        System.out.println("BMTree learning page size  : " + pageSize);
        System.out.println("BMTree min node size       : " + minNodeSize);

        buildInternal(outputPath, bitLength, maxDepth, learnedTree);
    }

    private static void buildInternal(String outputPath,
                                      int[] bitLength,
                                      int maxDepth,
                                      List<NodeRecord> learnedTree) throws IOException {
        validateBitLength(bitLength);

        List<NodeRecord> nodes = learnedTree != null ? learnedTree : buildZOrderTree(bitLength, maxDepth);
        nodes.sort(Comparator.comparingInt(node -> node.nodeId));

        Path output = Paths.get(outputPath);
        Path parent = output.getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }

        List<String> lines = new ArrayList<>(nodes.size() + 2);
        lines.add(formatHeader(bitLength));
        lines.add(String.valueOf(maxDepth));
        for (NodeRecord node : nodes) {
            lines.add(node.nodeId + " " + node.parentId + " " + node.depth + " "
                    + node.chosenBit + " " + node.leftChild + " " + node.rightChild + " " + node.child);
        }

        Files.write(output, lines, StandardCharsets.UTF_8);
        mirrorToClasspath(output, lines);
    }

    private static List<NodeRecord> buildZOrderTree(int[] bitLength, int maxDepth) {
        List<NodeRecord> nodes = new ArrayList<>();
        ArrayDeque<BuildState> queue = new ArrayDeque<>();
        queue.add(new BuildState(0, -1, 0, new int[bitLength.length], bitLength.length - 1));

        int nextId = 1;
        while (!queue.isEmpty()) {
            BuildState state = queue.removeFirst();
            boolean canSplit = state.depth < maxDepth && hasRemainingBits(state.dimensionChoose, bitLength);
            if (!canSplit) {
                nodes.add(new NodeRecord(state.nodeId, state.parentId, state.depth, -1, -1, -1, -1));
                continue;
            }

            int chosenBit = zOrderHeuristic(state.parentAction, state.dimensionChoose, bitLength);
            int[] childChoose = Arrays.copyOf(state.dimensionChoose, state.dimensionChoose.length);
            childChoose[chosenBit] += 1;
            int leftId = nextId++;
            int rightId = nextId++;

            nodes.add(new NodeRecord(state.nodeId, state.parentId, state.depth, chosenBit, leftId, rightId, -1));
            queue.addLast(new BuildState(leftId, state.nodeId, state.depth + 1, childChoose, chosenBit));
            queue.addLast(new BuildState(rightId, state.nodeId, state.depth + 1, childChoose, chosenBit));
        }
        return nodes;
    }

    private static List<NodeRecord> learnTree(int[] bitLength,
                                              int maxDepth,
                                              List<TrajectorySample> samples,
                                              List<QueryWindow> queries,
                                              int pageSize,
                                              int minNodeSize) {
        List<NodeRecord> nodes = new ArrayList<>();
        nodes.add(new NodeRecord(0, -1, 0, -1, -1, -1, -1));

        ArrayDeque<LearnState> queue = new ArrayDeque<>();
        int[] rootIndexes = new int[samples.size()];
        for (int i = 0; i < samples.size(); i++) {
            rootIndexes[i] = i;
        }
        queue.add(new LearnState(0, 0, new int[bitLength.length], bitLength.length - 1, rootIndexes));

        int nextId = 1;
        while (!queue.isEmpty()) {
            LearnState state = queue.removeFirst();
            NodeRecord node = nodes.get(state.nodeId);
            boolean canSplit = state.depth < maxDepth
                    && hasRemainingBits(state.dimensionChoose, bitLength)
                    && state.sampleIndexes.length > minNodeSize;
            if (!canSplit) {
                continue;
            }

            int bestDim = selectBestDimension(nodes, state, bitLength, samples, queries, pageSize);
            int[] childChoose = Arrays.copyOf(state.dimensionChoose, state.dimensionChoose.length);
            childChoose[bestDim] += 1;
            int bitIndex = bitLength[bestDim] - 1 - state.dimensionChoose[bestDim];

            IntList leftIndexes = new IntList(state.sampleIndexes.length);
            IntList rightIndexes = new IntList(state.sampleIndexes.length);
            for (int sampleIndex : state.sampleIndexes) {
                TrajectorySample sample = samples.get(sampleIndex);
                if (sample.repBits[bestDim][bitIndex] == 0) {
                    leftIndexes.add(sampleIndex);
                } else {
                    rightIndexes.add(sampleIndex);
                }
            }

            int leftId = nextId++;
            int rightId = nextId++;
            node.chosenBit = bestDim;
            node.leftChild = leftId;
            node.rightChild = rightId;

            nodes.add(new NodeRecord(leftId, state.nodeId, state.depth + 1, -1, -1, -1, -1));
            nodes.add(new NodeRecord(rightId, state.nodeId, state.depth + 1, -1, -1, -1, -1));

            queue.addLast(new LearnState(leftId, state.depth + 1, childChoose, bestDim, leftIndexes.toArray()));
            queue.addLast(new LearnState(rightId, state.depth + 1, childChoose, bestDim, rightIndexes.toArray()));
        }
        return nodes;
    }

    private static int selectBestDimension(List<NodeRecord> nodes,
                                           LearnState state,
                                           int[] bitLength,
                                           List<TrajectorySample> samples,
                                           List<QueryWindow> queries,
                                           int pageSize) {
        int fallback = zOrderHeuristic(state.parentAction, state.dimensionChoose, bitLength);
        int bestDim = fallback;
        double bestScore = Double.POSITIVE_INFINITY;

        for (int dim = 0; dim < bitLength.length; dim++) {
            if (state.dimensionChoose[dim] >= bitLength[dim]) {
                continue;
            }
            double score = computeAverageScanRange(nodes, samples, queries, pageSize, state.nodeId, dim, bitLength);
            if (score < bestScore - 1e-9
                    || (Math.abs(score - bestScore) <= 1e-9 && dim == fallback)) {
                bestDim = dim;
                bestScore = score;
            }
        }
        return bestDim;
    }

    private static double computeAverageScanRange(List<NodeRecord> nodes,
                                                  List<TrajectorySample> samples,
                                                  List<QueryWindow> queries,
                                                  int pageSize,
                                                  int overrideNodeId,
                                                  int overrideChosenBit,
                                                  int[] bitLength) {
        List<EvalEntry> entries = new ArrayList<>(samples.size());
        for (int i = 0; i < samples.size(); i++) {
            long value = computeCurveValue(samples.get(i), nodes, overrideNodeId, overrideChosenBit, bitLength);
            entries.add(new EvalEntry(i, value, samples.get(i)));
        }

        entries.sort(Comparator
                .comparingLong((EvalEntry entry) -> entry.value)
                .thenComparingInt(entry -> entry.sampleIndex));

        int[] samplePage = new int[samples.size()];
        for (int i = 0; i < entries.size(); i++) {
            samplePage[entries.get(i).sampleIndex] = i / Math.max(1, pageSize);
        }

        double totalScan = 0.0;
        for (QueryWindow query : queries) {
            int minPage = Integer.MAX_VALUE;
            int maxPage = Integer.MIN_VALUE;
            for (TrajectorySample sample : samples) {
                if (!query.intersects(sample)) {
                    continue;
                }
                int page = samplePage[sample.sampleId];
                minPage = Math.min(minPage, page);
                maxPage = Math.max(maxPage, page);
            }
            if (minPage == Integer.MAX_VALUE) {
                continue;
            }
            totalScan += maxPage - minPage + 1;
        }
        return totalScan / queries.size();
    }

    private static long computeCurveValue(TrajectorySample sample,
                                          List<NodeRecord> nodes,
                                          int overrideNodeId,
                                          int overrideChosenBit,
                                          int[] bitLength) {
        long value = 0L;
        int[] usedBits = new int[bitLength.length];
        int parentAction = bitLength.length - 1;
        NodeRecord current = nodes.get(0);

        while (current != null) {
            int chosenBit = current.nodeId == overrideNodeId ? overrideChosenBit : current.chosenBit;
            if (chosenBit < 0) {
                break;
            }

            int bitIndex = bitLength[chosenBit] - 1 - usedBits[chosenBit];
            if (bitIndex < 0) {
                break;
            }
            int bit = sample.repBits[chosenBit][bitIndex];
            value = (value << 1) | bit;
            usedBits[chosenBit] += 1;
            parentAction = chosenBit;

            int nextId = bit == 0 ? current.leftChild : current.rightChild;
            if (current.nodeId == overrideNodeId || nextId < 0 || nextId >= nodes.size()) {
                current = null;
            } else {
                current = nodes.get(nextId);
            }
        }

        int remaining = 0;
        for (int i = 0; i < bitLength.length; i++) {
            remaining += bitLength[i] - usedBits[i];
        }
        while (remaining > 0) {
            int nextAction = zOrderHeuristic(parentAction, usedBits, bitLength);
            int bitIndex = bitLength[nextAction] - 1 - usedBits[nextAction];
            int bit = sample.repBits[nextAction][bitIndex];
            value = (value << 1) | bit;
            usedBits[nextAction] += 1;
            parentAction = nextAction;
            remaining--;
        }
        return value;
    }

    private static List<RawTrajectorySample> loadTrajectorySamples(String sampleDataPath,
                                                                   int sampleLimit,
                                                                   Random random) throws IOException {
        Path path = Paths.get(sampleDataPath);
        if (!Files.exists(path)) {
            throw new IllegalArgumentException("Trajectory data file not found: " + sampleDataPath);
        }

        List<RawTrajectorySample> samples = new ArrayList<>();
        long seen = 0L;
        int failedLines = 0;
        for (String line : Files.readAllLines(path, StandardCharsets.UTF_8)) {
            String trimmed = line.trim();
            if (trimmed.isEmpty()) {
                continue;
            }
            try {
                TrajectoryParser.ParsedTrajectory parsed = TrajectoryParser.parse(trimmed);
                Envelope envelope = parsed.jtsGeo.getEnvelopeInternal();
                if (envelope == null || envelope.isNull()) {
                    continue;
                }
                RawTrajectorySample sample = new RawTrajectorySample(
                        envelope.getMinX(),
                        envelope.getMinY(),
                        envelope.getMaxX(),
                        envelope.getMaxY()
                );
                reservoirAdd(samples, sample, seen, sampleLimit, random);
                seen++;
            } catch (Exception ex) {
                failedLines++;
            }
        }
        System.out.println("BMTree trajectory samples loaded: " + samples.size() + ", failed lines: " + failedLines);
        return samples;
    }

    private static void reservoirAdd(List<RawTrajectorySample> samples,
                                     RawTrajectorySample sample,
                                     long seen,
                                     int sampleLimit,
                                     Random random) {
        if (sampleLimit <= 0) {
            samples.add(sample);
            return;
        }
        if (samples.size() < sampleLimit) {
            samples.add(sample);
            return;
        }
        long index = Math.abs(random.nextLong()) % (seen + 1L);
        if (index < sampleLimit) {
            samples.set((int) index, sample);
        }
    }

    private static List<QueryWindow> loadWorkloadSamples(String queryWorkloadPath, int sampleLimit) throws IOException {
        Path path = Paths.get(queryWorkloadPath);
        if (!Files.exists(path)) {
            throw new IllegalArgumentException("Query workload file not found: " + queryWorkloadPath);
        }

        List<QueryWindow> windows = queryWorkloadPath.endsWith(".json") ? loadJsonQueries(path) : loadTextQueries(path);
        if (windows.isEmpty()) {
            return windows;
        }

        List<QueryWindow> sampled = new ArrayList<>();
        int stride = sampleLimit > 0 && windows.size() > sampleLimit
                ? (int) Math.ceil(windows.size() / (double) sampleLimit)
                : 1;
        int limit = sampleLimit > 0 ? Math.min(sampleLimit, windows.size()) : windows.size();
        for (int i = 0; i < windows.size() && sampled.size() < limit; i += stride) {
            sampled.add(windows.get(i));
        }
        return sampled;
    }

    private static List<QueryWindow> loadJsonQueries(Path path) throws IOException {
        String content = new String(Files.readAllBytes(path), StandardCharsets.UTF_8);
        JSONArray jsonArray = new JSONArray(content);
        List<QueryWindow> windows = new ArrayList<>(jsonArray.length());
        for (int i = 0; i < jsonArray.length(); i++) {
            JSONObject obj = jsonArray.getJSONObject(i);
            String[] parts = obj.getString("query").split(",");
            if (parts.length == 4) {
                windows.add(new QueryWindow(
                        Double.parseDouble(parts[0].trim()),
                        Double.parseDouble(parts[1].trim()),
                        Double.parseDouble(parts[2].trim()),
                        Double.parseDouble(parts[3].trim())));
            }
        }
        return windows;
    }

    private static List<QueryWindow> loadTextQueries(Path path) throws IOException {
        List<QueryWindow> windows = new ArrayList<>();
        for (String line : Files.readAllLines(path, StandardCharsets.UTF_8)) {
            String trimmed = line.trim();
            if (trimmed.isEmpty()) {
                continue;
            }
            String[] parts = trimmed.split(",");
            if (parts.length == 4) {
                windows.add(new QueryWindow(
                        Double.parseDouble(parts[0].trim()),
                        Double.parseDouble(parts[1].trim()),
                        Double.parseDouble(parts[2].trim()),
                        Double.parseDouble(parts[3].trim())));
            }
        }
        return windows;
    }

    private static Bounds computeBounds(List<RawTrajectorySample> samples, List<QueryWindow> queries) {
        double minX = Double.POSITIVE_INFINITY;
        double minY = Double.POSITIVE_INFINITY;
        double maxX = Double.NEGATIVE_INFINITY;
        double maxY = Double.NEGATIVE_INFINITY;

        for (RawTrajectorySample sample : samples) {
            minX = Math.min(minX, sample.minLng);
            minY = Math.min(minY, sample.minLat);
            maxX = Math.max(maxX, sample.maxLng);
            maxY = Math.max(maxY, sample.maxLat);
        }
        for (QueryWindow query : queries) {
            minX = Math.min(minX, query.minLng);
            minY = Math.min(minY, query.minLat);
            maxX = Math.max(maxX, query.maxLng);
            maxY = Math.max(maxY, query.maxLat);
        }

        if (!(maxX > minX)) {
            minX = -180.0;
            maxX = 180.0;
        }
        if (!(maxY > minY)) {
            minY = -90.0;
            maxY = 90.0;
        }
        return new Bounds(minX, minY, maxX, maxY);
    }

    private static List<TrajectorySample> normalizeSamples(List<RawTrajectorySample> rawSamples,
                                                           int[] bitLength,
                                                           Bounds bounds) {
        int gridX = 1 << bitLength[0];
        int gridY = 1 << bitLength[1];
        List<TrajectorySample> samples = new ArrayList<>(rawSamples.size());
        for (int i = 0; i < rawSamples.size(); i++) {
            RawTrajectorySample sample = rawSamples.get(i);
            int repX = normalizeToGrid(sample.minLng, gridX, bounds.minX, bounds.maxX);
            int repY = normalizeToGrid(sample.minLat, gridY, bounds.minY, bounds.maxY);
            samples.add(new TrajectorySample(
                    i,
                    sample.minLng,
                    sample.minLat,
                    sample.maxLng,
                    sample.maxLat,
                    new int[][]{toBinary(repX, bitLength[0]), toBinary(repY, bitLength[1])}
            ));
        }
        return samples;
    }

    private static int zOrderHeuristic(int parentAction, int[] dimensionChoose, int[] bitLength) {
        int actionSpace = bitLength.length;
        int nextAction = (parentAction + 1) % actionSpace;
        for (int i = 0; i < actionSpace; i++) {
            int candidate = (nextAction + i) % actionSpace;
            if (dimensionChoose[candidate] < bitLength[candidate]) {
                return candidate;
            }
        }
        throw new IllegalStateException("No available dimension to choose");
    }

    private static boolean hasRemainingBits(int[] dimensionChoose, int[] bitLength) {
        for (int i = 0; i < bitLength.length; i++) {
            if (dimensionChoose[i] < bitLength[i]) {
                return true;
            }
        }
        return false;
    }

    private static int normalizeToGrid(double value, int gridSize, double min, double max) {
        if (max <= min) {
            return 0;
        }
        double normalized = (value - min) / (max - min);
        int cell = (int) Math.floor(normalized * gridSize);
        if (cell < 0) {
            return 0;
        }
        return Math.min(cell, gridSize - 1);
    }

    private static int[] toBinary(int value, int length) {
        int[] bits = new int[length];
        for (int i = 0; i < length; i++) {
            bits[i] = (value >> i) & 1;
        }
        return bits;
    }

    private static void validateBitLength(int[] bitLength) {
        if (bitLength == null || bitLength.length == 0) {
            throw new IllegalArgumentException("bitLength must not be empty");
        }
        for (int value : bitLength) {
            if (value <= 0) {
                throw new IllegalArgumentException("bitLength must be positive");
            }
        }
    }

    private static String formatHeader(int[] bitLength) {
        StringBuilder sb = new StringBuilder();
        sb.append(bitLength.length);
        for (int value : bitLength) {
            sb.append(' ').append(value);
        }
        return sb.toString();
    }

    private static Path resolveOutputPath(String configPath) {
        Path path = Paths.get(configPath);
        if (path.isAbsolute()) {
            return path;
        }
        String normalized = normalizeResourcePath(configPath);
        if (normalized.startsWith("src/main/resources/")) {
            return Paths.get(normalized);
        }
        return Paths.get("src/main/resources").resolve(normalized);
    }

    private static String normalizeResourcePath(String configPath) {
        return configPath.replace('\\', '/').replaceAll("^/+", "");
    }

    private static void mirrorResourceToFile(String resourcePath, Path outputPath) throws IOException {
        Path parent = outputPath.getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
        try (java.io.InputStream in = BMTreeConfigLearner.class.getClassLoader().getResourceAsStream(resourcePath)) {
            if (in == null) {
                throw new IOException("BMTree config resource not found: " + resourcePath);
            }
            Files.copy(in, outputPath, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
        }
    }

    private static void mirrorToClasspath(Path output, List<String> lines) throws IOException {
        String normalized = output.toString().replace('\\', '/');
        String marker = "src/main/resources/";
        int markerIndex = normalized.indexOf(marker);
        if (markerIndex < 0) {
            return;
        }
        String relative = normalized.substring(markerIndex + marker.length());
        Path classpathTarget = Paths.get("target/classes").resolve(relative);
        Path parent = classpathTarget.getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
        Files.write(classpathTarget, lines, StandardCharsets.UTF_8);
    }

    private static final class BuildState {
        private final int nodeId;
        private final int parentId;
        private final int depth;
        private final int[] dimensionChoose;
        private final int parentAction;

        private BuildState(int nodeId, int parentId, int depth, int[] dimensionChoose, int parentAction) {
            this.nodeId = nodeId;
            this.parentId = parentId;
            this.depth = depth;
            this.dimensionChoose = dimensionChoose;
            this.parentAction = parentAction;
        }
    }

    private static final class LearnState {
        private final int nodeId;
        private final int depth;
        private final int[] dimensionChoose;
        private final int parentAction;
        private final int[] sampleIndexes;

        private LearnState(int nodeId, int depth, int[] dimensionChoose, int parentAction, int[] sampleIndexes) {
            this.nodeId = nodeId;
            this.depth = depth;
            this.dimensionChoose = dimensionChoose;
            this.parentAction = parentAction;
            this.sampleIndexes = sampleIndexes;
        }
    }

    private static final class NodeRecord {
        private final int nodeId;
        private final int parentId;
        private final int depth;
        private int chosenBit;
        private int leftChild;
        private int rightChild;
        private final int child;

        private NodeRecord(int nodeId, int parentId, int depth, int chosenBit, int leftChild, int rightChild, int child) {
            this.nodeId = nodeId;
            this.parentId = parentId;
            this.depth = depth;
            this.chosenBit = chosenBit;
            this.leftChild = leftChild;
            this.rightChild = rightChild;
            this.child = child;
        }
    }

    private static final class RawTrajectorySample {
        private final double minLng;
        private final double minLat;
        private final double maxLng;
        private final double maxLat;

        private RawTrajectorySample(double minLng,
                                    double minLat,
                                    double maxLng,
                                    double maxLat) {
            this.minLng = minLng;
            this.minLat = minLat;
            this.maxLng = maxLng;
            this.maxLat = maxLat;
        }
    }

    private static final class TrajectorySample {
        private final int sampleId;
        private final double minLng;
        private final double minLat;
        private final double maxLng;
        private final double maxLat;
        private final int[][] repBits;

        private TrajectorySample(int sampleId,
                                 double minLng,
                                 double minLat,
                                 double maxLng,
                                 double maxLat,
                                 int[][] repBits) {
            this.sampleId = sampleId;
            this.minLng = minLng;
            this.minLat = minLat;
            this.maxLng = maxLng;
            this.maxLat = maxLat;
            this.repBits = repBits;
        }
    }

    private static final class QueryWindow {
        private final double minLng;
        private final double minLat;
        private final double maxLng;
        private final double maxLat;

        private QueryWindow(double minLng, double minLat, double maxLng, double maxLat) {
            this.minLng = minLng;
            this.minLat = minLat;
            this.maxLng = maxLng;
            this.maxLat = maxLat;
        }

        private boolean intersects(TrajectorySample sample) {
            return sample.maxLng >= minLng
                    && sample.minLng <= maxLng
                    && sample.maxLat >= minLat
                    && sample.minLat <= maxLat;
        }
    }

    private static final class Bounds {
        private final double minX;
        private final double minY;
        private final double maxX;
        private final double maxY;

        private Bounds(double minX, double minY, double maxX, double maxY) {
            this.minX = minX;
            this.minY = minY;
            this.maxX = maxX;
            this.maxY = maxY;
        }
    }

    private static final class EvalEntry {
        private final int sampleIndex;
        private final long value;
        private final TrajectorySample sample;

        private EvalEntry(int sampleIndex, long value, TrajectorySample sample) {
            this.sampleIndex = sampleIndex;
            this.value = value;
            this.sample = sample;
        }
    }

    private static final class IntList {
        private int[] values;
        private int size;

        private IntList(int initialCapacity) {
            this.values = new int[Math.max(4, initialCapacity)];
        }

        private void add(int value) {
            if (size == values.length) {
                values = Arrays.copyOf(values, values.length * 2);
            }
            values[size++] = value;
        }

        private int[] toArray() {
            return Arrays.copyOf(values, size);
        }
    }
}
