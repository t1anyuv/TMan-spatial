package utils;

import com.fasterxml.jackson.core.util.DefaultIndenter;
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Builds a minimal LMSFC order file from an existing LETI effective-node order file.
 * <p>
 * The implementation keeps the current project format:
 * - one ordering item per effective node
 * - quad_code / parent.element_code naming
 * - explicit effective_subtree_orders coverage
 */
public final class LMSFCOrderFileBuilder {

    public static final String DEFAULT_BASE_ORDER =
            "leti/tdrive/param/skewed_r8_min4_a3_b3.json";
    public static final String DEFAULT_OUTPUT_ORDER =
            "src/main/resources/lmsfc/tdrive/skewed_r8_min4_a3_b3.json";
    public static final String DEFAULT_THETA =
            "0, 1, 2, 3, 5, 7, 8, 9, 10, 11, 18, 19, 21, 24, 25, 26, 28, 29, 31, 36, " +
                    "4, 6, 12, 13, 14, 15, 16, 17, 20, 22, 23, 27, 30, 32, 33, 34, 35, 37, 38, 39";

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final DefaultPrettyPrinter PRETTY_PRINTER = createPrettyPrinter();

    private LMSFCOrderFileBuilder() {
    }

    public static void main(String[] args) throws Exception {
        String baseOrderPath = args.length >= 1 ? args[0].trim() : DEFAULT_BASE_ORDER;
        String thetaConfig = args.length >= 2 ? args[1].trim() : DEFAULT_THETA;
        String outputPath = args.length >= 3 ? args[2].trim() : DEFAULT_OUTPUT_ORDER;
        build(baseOrderPath, thetaConfig, outputPath);
        System.out.println("LMSFC order written to: " + outputPath);
    }

    public static void build(String baseOrderPath, String thetaConfig, String outputPath) throws IOException {
        ObjectNode sourceRoot = loadRoot(baseOrderPath);
        ArrayNode sourceOrdering = requireOrdering(sourceRoot, baseOrderPath);
        JsonNode metadata = sourceRoot.path("metadata");
        JsonNode boundary = metadata.path("spatial_boundary");

        double xMin = boundary.path("xmin").asDouble();
        double xMax = boundary.path("xmax").asDouble();
        double yMin = boundary.path("ymin").asDouble();
        double yMax = boundary.path("ymax").asDouble();

        Theta theta = Theta.parse(thetaConfig);
        int maxLevel = metadata.path("quadtree_max_level").asInt();

        List<Entry> entries = new ArrayList<>(sourceOrdering.size());
        for (JsonNode item : sourceOrdering) {
            JsonNode parent = item.path("parent");
            long quadCode = parent.path("element_code").asLong();
            int level = parent.path("level").asInt();
            double nodeXMin = parent.path("xmin").asDouble();
            double nodeYMin = parent.path("ymin").asDouble();

            int gridX = normalizeToGrid(nodeXMin, xMin, xMax, theta.gridSize);
            int gridY = normalizeToGrid(nodeYMin, yMin, yMax, theta.gridSize);
            long sortKey = calcSfcPosition(gridX, gridY, theta);

            entries.add(new Entry(
                    quadCode,
                    level,
                    intervalUpperBound(quadCode, level, maxLevel),
                    sortKey,
                    item.deepCopy()
            ));
        }

        entries.sort(Comparator
                .comparingLong((Entry entry) -> entry.sortKey)
                .thenComparingInt(entry -> entry.level)
                .thenComparingLong(entry -> entry.quadCode));

        for (int i = 0; i < entries.size(); i++) {
            Entry entry = entries.get(i);
            entry.order = i;
            entry.item.put("order", i);
            entry.item.put("quad_code", entry.quadCode);
            ObjectNode parent = (ObjectNode) entry.item.path("parent");
            parent.put("element_code", entry.quadCode);
            parent.remove("elementCode");
        }

        populateCoverage(entries);

        ArrayNode outputOrdering = MAPPER.createArrayNode();
        entries.stream()
                .sorted(Comparator.comparingInt(entry -> entry.order))
                .forEach(entry -> outputOrdering.add(entry.item));

        ObjectNode outputRoot = MAPPER.createObjectNode();
        outputRoot.set("ordering", outputOrdering);
        outputRoot.set("metadata", buildMetadata(metadata.deepCopy(), thetaConfig, entries.size()));
        writeRoot(outputRoot, outputPath);
    }

    private static ObjectNode buildMetadata(ObjectNode metadata, String thetaConfig, int activeCells) {
        metadata.put("active_cells", activeCells);
        metadata.put("order_source", "LMSFC");
        metadata.put("effective_subtree_contiguous", false);
        metadata.put("theta_config", thetaConfig);
        if (metadata.has("total_cells")) {
            long totalCells = metadata.path("total_cells").asLong();
            metadata.put("muted_cells", totalCells - activeCells);
        }
        return metadata;
    }

    private static void populateCoverage(List<Entry> entries) {
        List<Entry> byQuadCode = new ArrayList<>(entries);
        byQuadCode.sort(Comparator.comparingLong(entry -> entry.quadCode));

        ArrayDeque<Entry> stack = new ArrayDeque<>();
        List<Entry> roots = new ArrayList<>();
        for (Entry entry : byQuadCode) {
            while (!stack.isEmpty() && !isInside(entry, stack.peekLast())) {
                stack.removeLast();
            }
            if (stack.isEmpty()) {
                roots.add(entry);
            } else {
                stack.peekLast().children.add(entry);
            }
            stack.addLast(entry);
        }

        for (Entry root : roots) {
            computeSubtreeOrders(root);
        }
    }

    private static int[] computeSubtreeOrders(Entry entry) {
        List<Integer> orders = new ArrayList<>();
        orders.add(entry.order);
        for (Entry child : entry.children) {
            int[] childOrders = computeSubtreeOrders(child);
            for (int value : childOrders) {
                orders.add(value);
            }
        }
        orders.sort(Integer::compareTo);

        ArrayNode subtreeOrders = MAPPER.createArrayNode();
        for (int i = 1; i < orders.size(); i++) {
            subtreeOrders.add(orders.get(i));
        }

        ObjectNode coverage = MAPPER.createObjectNode();
        coverage.put("effective_subtree_count", Math.max(0, orders.size() - 1));
        coverage.set("effective_subtree_orders", subtreeOrders);
        entry.item.set("coverage", coverage);

        int[] allOrders = new int[orders.size()];
        for (int i = 0; i < orders.size(); i++) {
            allOrders[i] = orders.get(i);
        }
        return allOrders;
    }

    private static boolean isInside(Entry child, Entry parent) {
        return child.quadCode >= parent.quadCode && child.subtreeUpper <= parent.subtreeUpper;
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
        return Math.min(cell, gridSize - 1);
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

    private static long intervalUpperBound(long quadCode, int level, int maxLevel) throws IOException {
        if (level < 0 || maxLevel < level) {
            throw new IOException("Invalid level/maxLevel pair: level=" + level + ", maxLevel=" + maxLevel);
        }
        int exponent = maxLevel - level + 1;
        long power = 1L;
        for (int i = 0; i < exponent; i++) {
            if (power > Long.MAX_VALUE / 4L) {
                throw new IOException("Interval size overflow");
            }
            power *= 4L;
        }
        return quadCode + ((power - 1L) / 3L);
    }

    private static ObjectNode loadRoot(String inputPath) throws IOException {
        Path localPath = Paths.get(inputPath);
        if (Files.exists(localPath)) {
            return (ObjectNode) MAPPER.readTree(localPath.toFile());
        }

        String resourcePath = inputPath.replace('\\', '/').replaceAll("^/+", "");
        try (InputStream stream = LMSFCOrderFileBuilder.class.getClassLoader().getResourceAsStream(resourcePath)) {
            if (stream == null) {
                throw new IOException("Order file not found: " + inputPath);
            }
            return (ObjectNode) MAPPER.readTree(stream);
        }
    }

    private static ArrayNode requireOrdering(ObjectNode root, String sourcePath) throws IOException {
        JsonNode ordering = root.get("ordering");
        if (ordering == null || !ordering.isArray()) {
            throw new IOException("Missing ordering array in: " + sourcePath);
        }
        return (ArrayNode) ordering;
    }

    private static void writeRoot(ObjectNode root, String outputPath) throws IOException {
        File outputFile = new File(outputPath);
        File parent = outputFile.getParentFile();
        if (parent != null && !parent.exists() && !parent.mkdirs()) {
            throw new IOException("Failed to create output directory: " + parent.getAbsolutePath());
        }
        try (Writer writer = new OutputStreamWriter(Files.newOutputStream(outputFile.toPath()), StandardCharsets.UTF_8)) {
            MAPPER.writer(PRETTY_PRINTER).writeValue(writer, root);
        }
    }

    private static DefaultPrettyPrinter createPrettyPrinter() {
        DefaultPrettyPrinter printer = new DefaultPrettyPrinter();
        DefaultIndenter indenter = new DefaultIndenter("  ", DefaultIndenter.SYS_LF);
        printer.indentObjectsWith(indenter);
        printer.indentArraysWith(indenter);
        return printer;
    }

    private static final class Theta {
        private final int bitNum;
        private final int gridSize;
        private final long[] thetaX;
        private final long[] thetaY;

        private Theta(int bitNum, int gridSize, long[] thetaX, long[] thetaY) {
            this.bitNum = bitNum;
            this.gridSize = gridSize;
            this.thetaX = thetaX;
            this.thetaY = thetaY;
        }

        private static Theta parse(String thetaConfig) {
            String[] parts = thetaConfig.split(",");
            if (parts.length == 0 || parts.length % 2 != 0) {
                throw new IllegalArgumentException("Invalid theta config: " + thetaConfig);
            }

            int bitNum = parts.length / 2;
            long[] thetaX = new long[bitNum];
            long[] thetaY = new long[bitNum];
            for (int i = 0; i < bitNum; i++) {
                thetaX[i] = 1L << Integer.parseInt(parts[i].trim());
                thetaY[i] = 1L << Integer.parseInt(parts[i + bitNum].trim());
            }
            return new Theta(bitNum, 1 << bitNum, thetaX, thetaY);
        }
    }

    private static final class Entry {
        private final long quadCode;
        private final int level;
        private final long subtreeUpper;
        private final long sortKey;
        private final ObjectNode item;
        private final List<Entry> children = new ArrayList<>();
        private int order;

        private Entry(long quadCode, int level, long subtreeUpper, long sortKey, ObjectNode item) {
            this.quadCode = quadCode;
            this.level = level;
            this.subtreeUpper = subtreeUpper;
            this.sortKey = sortKey;
            this.item = item;
        }
    }
}
