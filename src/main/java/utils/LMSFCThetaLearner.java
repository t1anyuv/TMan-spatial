package utils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Getter;
import org.locationtech.jts.geom.Coordinate;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

/**
 * A compact search-based LMSFC theta learner driven only by sampled trajectory points.
 */
public final class LMSFCThetaLearner {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final int DEFAULT_CANDIDATE_COUNT = 64;
    private static final int DEFAULT_POINT_SAMPLE_LIMIT = 10_000;
    private static final int DEFAULT_EVALUATION_WINDOW_LIMIT = 300;
    private static final int DEFAULT_THETA_BIT_NUM = 20;
    private static final long DEFAULT_SEED = 20260421L;
    private static final double DEFAULT_WINDOW_RATIO = 0.01d;

    private LMSFCThetaLearner() {
    }

    public static void main(String[] args) throws Exception {
        SearchConfig config = SearchConfig.fromArgs(args);
        if (config.sampleDataPath.isEmpty()) {
            printUsage();
            throw new IllegalArgumentException("sample_data_path is required");
        }

        SearchResult result = learn(config);

        System.out.println("LMSFC theta learning finished");
        System.out.println("sample source: " + result.sampleSource);
        System.out.println("sample count : " + result.sampleCount);
        System.out.println("best score   : " + formatDouble(result.bestScore));
        System.out.println("avg scanned  : " + formatDouble(result.avgScanned));
        System.out.println("avg falsePos : " + formatDouble(result.avgFalsePositive));
        System.out.println("best theta   : " + result.bestThetaConfig);

        if (!config.outputOrderPath.isEmpty()) {
            LMSFCOrderFileBuilder.build(config.baseOrderPath, result.bestThetaConfig, config.outputOrderPath);
            System.out.println("order output : " + config.outputOrderPath);
        }
    }

    public static SearchResult learn(SearchConfig config) throws IOException {
        if (config.sampleDataPath == null || config.sampleDataPath.trim().isEmpty()) {
            throw new IllegalArgumentException("Real trajectory data path is required for theta learning");
        }

        Path samplePath = Paths.get(config.sampleDataPath);
        if (!Files.exists(samplePath)) {
            throw new IOException("Trajectory data file not found: " + config.sampleDataPath);
        }

        Bounds bounds = loadBounds(config.baseOrderPath);
        int bitNum = config.thetaBitNum;

        Random sampleRandom = new Random(config.seed);
        List<RawPoint> rawSamples = loadTrajectoryPoints(samplePath, config.pointSampleLimit, sampleRandom);
        if (rawSamples.isEmpty()) {
            throw new IOException("No sample points available for LMSFC theta learning");
        }

        List<GridPoint> gridPoints = normalizeSamples(rawSamples, bounds, bitNum);
        if (gridPoints.isEmpty()) {
            throw new IOException("No valid sample points remain after normalization");
        }

        List<QueryWindow> windows = buildEvaluationWindows(gridPoints, bitNum, config.seed);
        if (windows.isEmpty()) {
            throw new IOException("No evaluation windows available for LMSFC theta learning");
        }

        SearchState searchState = new SearchState();
        Random searchRandom = new Random(config.seed ^ 0x9E3779B97F4A7C15L);

        int[][] warmupCandidates = buildWarmupCandidates(bitNum, config.initialThetaConfig);
        for (int[] layout : warmupCandidates) {
            if (searchState.evaluatedCount >= config.candidateCount) {
                break;
            }
            evaluateCandidate(layout, gridPoints, windows, searchState);
        }

        while (searchState.evaluatedCount < config.candidateCount) {
            int[] nextLayout;
            if (searchState.bestLayout != null && searchRandom.nextDouble() < 0.70d) {
                nextLayout = mutateLayout(searchState.bestLayout, searchRandom);
            } else {
                nextLayout = randomLayout(bitNum, searchRandom);
            }
            evaluateCandidate(nextLayout, gridPoints, windows, searchState);
        }

        return new SearchResult(
                config.sampleDataPath,
                gridPoints.size(),
                layoutToThetaConfig(searchState.bestLayout),
                searchState.bestScore,
                searchState.bestAvgScanned,
                searchState.bestAvgFalsePositive
        );
    }

    public static SearchResult buildLearnedOrder(String sampleDataPath,
                                                 String baseOrderPath,
                                                 String outputOrderPath,
                                                 int thetaBitNum,
                                                 int candidateCount,
                                                 int pointSampleLimit,
                                                 long seed) throws IOException {
        SearchConfig config = new SearchConfig(
                sampleDataPath,
                baseOrderPath,
                thetaBitNum,
                candidateCount,
                pointSampleLimit,
                seed,
                outputOrderPath,
                buildDefaultTheta(thetaBitNum)
        );
        SearchResult result = learn(config);
        LMSFCOrderFileBuilder.build(baseOrderPath, result.bestThetaConfig, outputOrderPath);
        return result;
    }

    private static void evaluateCandidate(int[] layout,
                                          List<GridPoint> gridPoints,
                                          List<QueryWindow> windows,
                                          SearchState state) {
        if (layout == null) {
            return;
        }

        String signature = Arrays.toString(layout);
        if (!state.seenLayouts.add(signature)) {
            return;
        }

        Theta theta = Theta.fromLayout(layout);
        CandidatePoint[] curve = new CandidatePoint[gridPoints.size()];
        for (int i = 0; i < gridPoints.size(); i++) {
            GridPoint point = gridPoints.get(i);
            curve[i] = new CandidatePoint(point.x, point.y, calcSfcPosition(point.x, point.y, theta));
        }
        Arrays.sort(curve, Comparator.comparingLong(left -> left.sfc));

        double totalScanned = 0d;
        double totalFalsePositive = 0d;
        for (QueryWindow window : windows) {
            long start = calcSfcPosition(window.xMin, window.yMin, theta);
            long end = calcSfcPosition(window.xMax, window.yMax, theta);
            if (start > end) {
                long temp = start;
                start = end;
                end = temp;
            }

            int left = lowerBound(curve, start);
            int right = upperBound(curve, end);
            int scanned = Math.max(0, right - left);

            int falsePositive = 0;
            for (int i = left; i < right; i++) {
                CandidatePoint point = curve[i];
                if (!window.contains(point.x, point.y)) {
                    falsePositive++;
                }
            }

            totalScanned += scanned;
            totalFalsePositive += falsePositive;
        }

        state.evaluatedCount++;
        double avgScanned = totalScanned / windows.size();
        double avgFalsePositive = totalFalsePositive / windows.size();
        double score = avgScanned + avgFalsePositive;

        if (score < state.bestScore) {
            state.bestScore = score;
            state.bestAvgScanned = avgScanned;
            state.bestAvgFalsePositive = avgFalsePositive;
            state.bestLayout = Arrays.copyOf(layout, layout.length);
        }
    }

    private static int[][] buildWarmupCandidates(int bitNum, String initialThetaConfig) {
        List<int[]> layouts = new ArrayList<>();
        layouts.add(thetaConfigToLayout(initialThetaConfig));
        layouts.add(alternatingLayout(bitNum, true));
        layouts.add(alternatingLayout(bitNum, false));
        layouts.add(blockLayout(bitNum, true));
        layouts.add(blockLayout(bitNum, false));
        return layouts.toArray(new int[0][]);
    }

    private static int[] alternatingLayout(int bitNum, boolean xFirst) {
        int[] layout = new int[bitNum * 2];
        for (int i = 0; i < layout.length; i++) {
            boolean useX = (i % 2 == 0) == xFirst;
            layout[i] = useX ? 0 : 1;
        }
        return layout;
    }

    private static int[] blockLayout(int bitNum, boolean xFirst) {
        int[] layout = new int[bitNum * 2];
        for (int i = 0; i < layout.length; i++) {
            layout[i] = i < bitNum ? (xFirst ? 0 : 1) : (xFirst ? 1 : 0);
        }
        return layout;
    }

    private static int[] randomLayout(int bitNum, Random random) {
        int totalBits = bitNum * 2;
        int[] layout = new int[totalBits];
        int xRemaining = bitNum;
        int yRemaining = bitNum;
        for (int i = 0; i < totalBits; i++) {
            if (xRemaining == 0) {
                layout[i] = 1;
                yRemaining--;
            } else if (yRemaining == 0) {
                layout[i] = 0;
                xRemaining--;
            } else {
                int draw = random.nextInt(xRemaining + yRemaining);
                if (draw < xRemaining) {
                    layout[i] = 0;
                    xRemaining--;
                } else {
                    layout[i] = 1;
                    yRemaining--;
                }
            }
        }
        return layout;
    }

    private static int[] mutateLayout(int[] baseLayout, Random random) {
        int[] mutated = Arrays.copyOf(baseLayout, baseLayout.length);
        List<Integer> xPositions = new ArrayList<>();
        List<Integer> yPositions = new ArrayList<>();
        for (int i = 0; i < mutated.length; i++) {
            if (mutated[i] == 0) {
                xPositions.add(i);
            } else {
                yPositions.add(i);
            }
        }
        if (xPositions.isEmpty() || yPositions.isEmpty()) {
            return mutated;
        }

        int xIndex = xPositions.get(random.nextInt(xPositions.size()));
        int yIndex = yPositions.get(random.nextInt(yPositions.size()));
        mutated[xIndex] = 1;
        mutated[yIndex] = 0;
        return mutated;
    }

    private static List<QueryWindow> buildEvaluationWindows(List<GridPoint> gridPoints,
                                                            int bitNum,
                                                            long seed) {
        int windowCount = Math.min(DEFAULT_EVALUATION_WINDOW_LIMIT, gridPoints.size());
        List<QueryWindow> windows = new ArrayList<>(windowCount);
        int gridSize = 1 << bitNum;
        int radius = Math.max(1, (int) Math.round(gridSize * DEFAULT_WINDOW_RATIO / 2d));
        Random random = new Random(seed ^ 0x5DEECE66DL);

        for (int i = 0; i < windowCount; i++) {
            GridPoint pivot = gridPoints.get(random.nextInt(gridPoints.size()));
            int xMin = Math.max(0, pivot.x - radius);
            int xMax = Math.min(gridSize - 1, pivot.x + radius);
            int yMin = Math.max(0, pivot.y - radius);
            int yMax = Math.min(gridSize - 1, pivot.y + radius);
            windows.add(new QueryWindow(xMin, xMax, yMin, yMax));
        }
        return windows;
    }

    private static List<GridPoint> normalizeSamples(List<RawPoint> rawSamples, Bounds bounds, int bitNum) {
        int gridSize = 1 << bitNum;
        List<GridPoint> gridPoints = new ArrayList<>(rawSamples.size());
        for (RawPoint sample : rawSamples) {
            int x = normalizeToGrid(sample.x, bounds.xMin, bounds.xMax, gridSize);
            int y = normalizeToGrid(sample.y, bounds.yMin, bounds.yMax, gridSize);
            gridPoints.add(new GridPoint(x, y));
        }
        return gridPoints;
    }

    private static List<RawPoint> loadTrajectoryPoints(Path path, int sampleLimit, Random random) throws IOException {
        List<RawPoint> samples = new ArrayList<>(sampleLimit);
        long seen = 0L;
        int failedLines = 0;

        try (BufferedReader reader = Files.newBufferedReader(path, StandardCharsets.UTF_8)) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.trim().isEmpty()) {
                    continue;
                }
                try {
                    TrajectoryParser.ParsedTrajectory parsed = TrajectoryParser.parse(line);
                    Coordinate[] coordinates = parsed.jtsGeo.getCoordinates();
                    for (Coordinate coordinate : coordinates) {
                        seen++;
                        RawPoint point = new RawPoint(coordinate.getX(), coordinate.getY());
                        reservoirAdd(samples, point, seen, sampleLimit, random);
                    }
                } catch (Exception ex) {
                    failedLines++;
                }
            }
        }

        System.out.println("trajectory samples loaded: " + samples.size() + ", failed lines: " + failedLines);
        return samples;
    }

    private static void reservoirAdd(List<RawPoint> samples,
                                     RawPoint point,
                                     long seen,
                                     int sampleLimit,
                                     Random random) {
        if (samples.size() < sampleLimit) {
            samples.add(point);
            return;
        }

        long index = nextLong(random, seen);
        if (index < sampleLimit) {
            samples.set((int) index, point);
        }
    }

    private static long nextLong(Random random, long bound) {
        if (bound <= Integer.MAX_VALUE) {
            return random.nextInt((int) bound);
        }
        long bits;
        long value;
        do {
            bits = random.nextLong() & Long.MAX_VALUE;
            value = bits % bound;
        } while (bits - value + (bound - 1) < 0L);
        return value;
    }

    private static Bounds loadBounds(String baseOrderPath) throws IOException {
        JsonNode root = loadRoot(baseOrderPath);
        JsonNode boundary = root.path("metadata").path("spatial_boundary");
        if (boundary.isMissingNode()) {
            throw new IOException("Missing spatial_boundary metadata in: " + baseOrderPath);
        }
        return new Bounds(
                boundary.path("xmin").asDouble(),
                boundary.path("xmax").asDouble(),
                boundary.path("ymin").asDouble(),
                boundary.path("ymax").asDouble()
        );
    }

    private static JsonNode loadRoot(String inputPath) throws IOException {
        Path localPath = Paths.get(inputPath);
        if (Files.exists(localPath)) {
            return MAPPER.readTree(localPath.toFile());
        }

        String resourcePath = inputPath.replace('\\', '/').replaceAll("^/+", "");
        try (InputStream stream = LMSFCThetaLearner.class.getClassLoader().getResourceAsStream(resourcePath)) {
            if (stream == null) {
                throw new IOException("File not found: " + inputPath);
            }
            return MAPPER.readTree(stream);
        }
    }

    private static int[] thetaConfigToLayout(String thetaConfig) {
        String[] parts = thetaConfig.split(",");
        int bitNum = parts.length / 2;
        int[] layout = new int[bitNum * 2];
        Arrays.fill(layout, -1);

        for (int i = 0; i < bitNum; i++) {
            layout[Integer.parseInt(parts[i].trim())] = 0;
            layout[Integer.parseInt(parts[i + bitNum].trim())] = 1;
        }
        for (int value : layout) {
            if (value < 0) {
                throw new IllegalArgumentException("Invalid theta config layout: " + thetaConfig);
            }
        }
        return layout;
    }

    private static String buildDefaultTheta(int bitNum) {
        List<Integer> xPositions = new ArrayList<>(bitNum);
        List<Integer> yPositions = new ArrayList<>(bitNum);
        for (int i = 0; i < bitNum * 2; i++) {
            if (i % 2 == 0) {
                xPositions.add(i);
            } else {
                yPositions.add(i);
            }
        }
        StringBuilder builder = new StringBuilder();
        appendValues(builder, xPositions);
        builder.append(", ");
        appendValues(builder, yPositions);
        return builder.toString();
    }

    private static String layoutToThetaConfig(int[] layout) {
        List<Integer> xPositions = new ArrayList<>();
        List<Integer> yPositions = new ArrayList<>();
        for (int i = 0; i < layout.length; i++) {
            if (layout[i] == 0) {
                xPositions.add(i);
            } else {
                yPositions.add(i);
            }
        }

        StringBuilder sb = new StringBuilder();
        appendValues(sb, xPositions);
        if (!yPositions.isEmpty()) {
            if (sb.length() > 0) {
                sb.append(", ");
            }
            appendValues(sb, yPositions);
        }
        return sb.toString();
    }

    private static void appendValues(StringBuilder sb, List<Integer> values) {
        for (int i = 0; i < values.size(); i++) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append(values.get(i));
        }
    }

    private static int normalizeToGrid(double value, double min, double max, int gridSize) {
        if (max <= min) {
            return 0;
        }
        double normalized = (value - min) / (max - min);
        int cell = (int) Math.floor(normalized * gridSize);
        if (cell < 0) {
            return 0;
        }
        if (cell >= gridSize) {
            return gridSize - 1;
        }
        return cell;
    }

    private static long calcSfcPosition(int x, int y, Theta theta) {
        long xValue = 0L;
        long yValue = 0L;
        for (int i = 0; i < theta.bitNum; i++) {
            xValue += ((long) ((x >>> i) & 1)) * theta.thetaX[i];
            yValue += ((long) ((y >>> i) & 1)) * theta.thetaY[i];
        }
        return xValue + yValue;
    }

    private static int lowerBound(CandidatePoint[] points, long target) {
        int left = 0;
        int right = points.length;
        while (left < right) {
            int mid = (left + right) >>> 1;
            if (points[mid].sfc < target) {
                left = mid + 1;
            } else {
                right = mid;
            }
        }
        return left;
    }

    private static int upperBound(CandidatePoint[] points, long target) {
        int left = 0;
        int right = points.length;
        while (left < right) {
            int mid = (left + right) >>> 1;
            if (points[mid].sfc <= target) {
                left = mid + 1;
            } else {
                right = mid;
            }
        }
        return left;
    }

    private static String formatDouble(double value) {
        return String.format("%.4f", value);
    }

    private static void printUsage() {
        System.out.println("Usage:");
        System.out.println("  LMSFCThetaLearner <sample_data_path> <base_order_path> <candidate_count>");
        System.out.println("                    <point_sample_limit> <seed> [output_order_path] [theta_bit_num]");
    }

    public static final class SearchConfig {
        private final String sampleDataPath;
        private final String baseOrderPath;
        private final int thetaBitNum;
        private final int candidateCount;
        private final int pointSampleLimit;
        private final long seed;
        private final String outputOrderPath;
        private final String initialThetaConfig;

        private SearchConfig(String sampleDataPath,
                             String baseOrderPath,
                             int thetaBitNum,
                             int candidateCount,
                             int pointSampleLimit,
                             long seed,
                             String outputOrderPath,
                             String initialThetaConfig) {
            this.sampleDataPath = sampleDataPath;
            this.baseOrderPath = baseOrderPath;
            this.thetaBitNum = thetaBitNum;
            this.candidateCount = candidateCount;
            this.pointSampleLimit = pointSampleLimit;
            this.seed = seed;
            this.outputOrderPath = outputOrderPath;
            this.initialThetaConfig = initialThetaConfig;
        }

        public static SearchConfig fromArgs(String[] args) {
            String sampleDataPath = args.length >= 1 ? args[0].trim() : "";
            String baseOrderPath = args.length >= 2 ? args[1].trim() : LMSFCOrderFileBuilder.DEFAULT_BASE_ORDER;
            int candidateCount = args.length >= 3 ? Integer.parseInt(args[2].trim()) : DEFAULT_CANDIDATE_COUNT;
            int pointSampleLimit = args.length >= 4 ? Integer.parseInt(args[3].trim()) : DEFAULT_POINT_SAMPLE_LIMIT;
            long seed = args.length >= 5 ? Long.parseLong(args[4].trim()) : DEFAULT_SEED;
            String outputOrderPath = args.length >= 6 ? args[5].trim() : "";
            int thetaBitNum = args.length >= 7 ? Integer.parseInt(args[6].trim()) : DEFAULT_THETA_BIT_NUM;

            return new SearchConfig(
                    sampleDataPath,
                    baseOrderPath,
                    thetaBitNum,
                    candidateCount,
                    pointSampleLimit,
                    seed,
                    outputOrderPath,
                    buildDefaultTheta(thetaBitNum)
            );
        }
    }

    public static final class SearchResult {
        private final String sampleSource;
        private final int sampleCount;
        @Getter
        private final String bestThetaConfig;
        private final double bestScore;
        private final double avgScanned;
        private final double avgFalsePositive;

        private SearchResult(String sampleSource,
                             int sampleCount,
                             String bestThetaConfig,
                             double bestScore,
                             double avgScanned,
                             double avgFalsePositive) {
            this.sampleSource = sampleSource;
            this.sampleCount = sampleCount;
            this.bestThetaConfig = bestThetaConfig;
            this.bestScore = bestScore;
            this.avgScanned = avgScanned;
            this.avgFalsePositive = avgFalsePositive;
        }
    }

    private static final class SearchState {
        private final Set<String> seenLayouts = new HashSet<>();
        private int evaluatedCount;
        private int[] bestLayout;
        private double bestScore = Double.POSITIVE_INFINITY;
        private double bestAvgScanned = Double.POSITIVE_INFINITY;
        private double bestAvgFalsePositive = Double.POSITIVE_INFINITY;
    }

    private static final class Bounds {
        private final double xMin;
        private final double xMax;
        private final double yMin;
        private final double yMax;

        private Bounds(double xMin, double xMax, double yMin, double yMax) {
            this.xMin = xMin;
            this.xMax = xMax;
            this.yMin = yMin;
            this.yMax = yMax;
        }
    }

    private static final class RawPoint {
        private final double x;
        private final double y;

        private RawPoint(double x, double y) {
            this.x = x;
            this.y = y;
        }
    }

    private static final class GridPoint {
        private final int x;
        private final int y;

        private GridPoint(int x, int y) {
            this.x = x;
            this.y = y;
        }
    }

    private static final class QueryWindow {
        private final int xMin;
        private final int xMax;
        private final int yMin;
        private final int yMax;

        private QueryWindow(int xMin, int xMax, int yMin, int yMax) {
            this.xMin = xMin;
            this.xMax = xMax;
            this.yMin = yMin;
            this.yMax = yMax;
        }

        private boolean contains(int x, int y) {
            return x >= xMin && x <= xMax && y >= yMin && y <= yMax;
        }
    }

    private static final class CandidatePoint {
        private final int x;
        private final int y;
        private final long sfc;

        private CandidatePoint(int x, int y, long sfc) {
            this.x = x;
            this.y = y;
            this.sfc = sfc;
        }
    }

    private static final class Theta {
        private final int bitNum;
        private final long[] thetaX;
        private final long[] thetaY;

        private Theta(int bitNum, long[] thetaX, long[] thetaY) {
            this.bitNum = bitNum;
            this.thetaX = thetaX;
            this.thetaY = thetaY;
        }

        private static Theta fromLayout(int[] layout) {
            int bitNum = layout.length / 2;
            long[] thetaX = new long[bitNum];
            long[] thetaY = new long[bitNum];
            int xCount = 0;
            int yCount = 0;
            for (int i = 0; i < layout.length; i++) {
                if (layout[i] == 0) {
                    thetaX[xCount++] = 1L << i;
                } else {
                    thetaY[yCount++] = 1L << i;
                }
            }
            return new Theta(bitNum, thetaX, thetaY);
        }
    }
}
