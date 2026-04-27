package utils;

import com.fasterxml.jackson.core.util.DefaultIndenter;
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import experiments.common.io.ExperimentPaths;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Builds a LETI-compatible order file whose qOrder follows a BMTree layout.
 */
public final class BMTreeOrderFileBuilder {

    public static final String DEFAULT_BASE_ORDER = "leti/tdrive/param/skewed_r8_min4_a3_b3.json";
    public static final String DEFAULT_OUTPUT_ORDER = "src/main/resources/bmtree/tdrive/skewed_r8_min4_a3_b3.json";

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final DefaultPrettyPrinter PRETTY_PRINTER = createPrettyPrinter();

    private BMTreeOrderFileBuilder() {
    }

    public static String ensureOrderExists(String baseOrderPath,
                                           String bmtreeConfigPath,
                                           String outputOrderPath,
                                           int[] bitLength,
                                           int maxDepth) throws IOException {
        Path output = Paths.get(outputOrderPath);
        if (Files.exists(output)) {
            return output.toString();
        }
        String ensuredConfigPath = BMTreeConfigLearner.ensureConfigExists(bmtreeConfigPath, bitLength, maxDepth);
        build(baseOrderPath, ensuredConfigPath, bitLength, outputOrderPath);
        return output.toString();
    }

    public static String ensureLearnedOrderExists(String baseOrderPath,
                                                  String bmtreeConfigPath,
                                                  String outputOrderPath,
                                                  int[] bitLength,
                                                  int maxDepth,
                                                  String sampleDataPath,
                                                  String queryWorkloadPath,
                                                  int trajectorySampleLimit,
                                                  int querySampleLimit,
                                                  int pageSize,
                                                  int minNodeSize,
                                                  long seed) throws IOException {
        Path output = Paths.get(outputOrderPath);
        if (Files.exists(output)) {
            return output.toString();
        }
        String ensuredConfigPath = BMTreeConfigLearner.ensureLearnedConfigExists(
                bmtreeConfigPath,
                bitLength,
                maxDepth,
                sampleDataPath,
                queryWorkloadPath,
                trajectorySampleLimit,
                querySampleLimit,
                pageSize,
                minNodeSize,
                seed);
        build(baseOrderPath, ensuredConfigPath, bitLength, outputOrderPath);
        return output.toString();
    }

    public static String ensureLearnedOrderResource(String baseOrderPath,
                                                    int[] bitLength,
                                                    int maxDepth,
                                                    String sampleDataPath,
                                                    String queryWorkloadPath,
                                                    int trajectorySampleLimit,
                                                    int querySampleLimit,
                                                    int pageSize,
                                                    int minNodeSize,
                                                    long seed) throws IOException {
        String dataset = resolveDataset(baseOrderPath);
        String orderFileName = resolveOrderFileName(baseOrderPath);
        String resourceOrderPath = String.format("bmtree/%s/%s", dataset, orderFileName);
        Path orderOutput = Paths.get("src/main/resources").resolve(resourceOrderPath);
        if (Files.exists(orderOutput)) {
            return resourceOrderPath;
        }

        String configFileName = "bmtree.txt";
        Path configOutput = Paths.get("src/main/resources", "bmtree", dataset, configFileName);
        String ensuredConfigPath = BMTreeConfigLearner.ensureLearnedConfigExists(
                configOutput.toString(),
                bitLength,
                maxDepth,
                sampleDataPath,
                queryWorkloadPath,
                trajectorySampleLimit,
                querySampleLimit,
                pageSize,
                minNodeSize,
                seed);
        build(baseOrderPath, ensuredConfigPath, bitLength, orderOutput.toString());
        return resourceOrderPath;
    }

    public static void build(String baseOrderPath,
                             String bmtreeConfigPath,
                             int[] bitLength,
                             String outputOrderPath) throws IOException {
        ObjectNode sourceRoot = loadRoot(baseOrderPath);
        ArrayNode sourceOrdering = requireOrdering(sourceRoot, baseOrderPath);
        ObjectNode metadata = (ObjectNode) sourceRoot.path("metadata").deepCopy();
        JsonNode boundary = metadata.path("spatial_boundary");

        double xMin = boundary.path("xmin").asDouble();
        double xMax = boundary.path("xmax").asDouble();
        double yMin = boundary.path("ymin").asDouble();
        double yMax = boundary.path("ymax").asDouble();
        int maxLevel = metadata.path("quadtree_max_level").asInt();

        BMTreeCurve curve = BMTreeCurve.load(bmtreeConfigPath, bitLength);

        List<Entry> entries = new ArrayList<>(sourceOrdering.size());
        for (JsonNode item : sourceOrdering) {
            ObjectNode cloned = item.deepCopy();
            JsonNode parent = cloned.path("parent");
            long quadCode = readLong(parent, "element_code", "elementCode");
            int level = parent.path("level").asInt();
            double nodeXMin = parent.path("xmin").asDouble();
            double nodeYMin = parent.path("ymin").asDouble();

            int gridX = normalizeToGrid(nodeXMin, xMin, xMax, curve.gridSizeX);
            int gridY = normalizeToGrid(nodeYMin, yMin, yMax, curve.gridSizeY);
            long sortKey = curve.position(gridX, gridY);

            entries.add(new Entry(
                    quadCode,
                    level,
                    intervalUpperBound(quadCode, level, maxLevel),
                    sortKey,
                    cloned
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

        metadata.put("active_cells", entries.size());
        if (metadata.has("total_cells")) {
            long totalCells = metadata.path("total_cells").asLong();
            metadata.put("muted_cells", totalCells - entries.size());
        }
        metadata.put("order_source", "BMTree");
        metadata.put("effective_subtree_contiguous", false);
        metadata.put("bmtree_config_path", bmtreeConfigPath.replace('\\', '/'));
        metadata.put("bmtree_bit_length", joinBitLength(bitLength));

        ObjectNode outputRoot = MAPPER.createObjectNode();
        outputRoot.set("ordering", outputOrdering);
        outputRoot.set("metadata", metadata);
        writeRoot(outputRoot, outputOrderPath);
    }

    private static String joinBitLength(int[] bitLength) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < bitLength.length; i++) {
            if (i > 0) {
                sb.append(',');
            }
            sb.append(bitLength[i]);
        }
        return sb.toString();
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

    private static long readLong(JsonNode node, String... fieldNames) {
        for (String fieldName : fieldNames) {
            JsonNode value = node.get(fieldName);
            if (value != null && !value.isNull() && !value.isMissingNode()) {
                return value.asLong();
            }
        }
        return Long.MIN_VALUE;
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
        String normalized = inputPath.trim();
        if (ExperimentPaths.isDistributedPath(normalized) || Files.exists(Paths.get(normalized))) {
            return (ObjectNode) MAPPER.readTree(ExperimentPaths.readUtf8String(normalized));
        }

        String resourcePath = normalized.replace('\\', '/').replaceAll("^/+", "");
        try (InputStream stream = BMTreeOrderFileBuilder.class.getClassLoader().getResourceAsStream(resourcePath)) {
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
        mirrorToClasspath(outputFile.toPath());
    }

    private static void mirrorToClasspath(Path output) throws IOException {
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
        Files.copy(output, classpathTarget, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
    }

    private static DefaultPrettyPrinter createPrettyPrinter() {
        DefaultPrettyPrinter printer = new DefaultPrettyPrinter();
        DefaultIndenter indenter = new DefaultIndenter("  ", DefaultIndenter.SYS_LF);
        printer.indentObjectsWith(indenter);
        printer.indentArraysWith(indenter);
        return printer;
    }

    private static String resolveDataset(String baseOrderPath) {
        String normalized = baseOrderPath.replace('\\', '/');
        String[] parts = normalized.split("/");
        for (int i = 0; i < parts.length - 1; i++) {
            if ("leti".equals(parts[i]) && i + 1 < parts.length) {
                return parts[i + 1].toLowerCase();
            }
        }
        return "tdrive";
    }

    private static String resolveOrderFileName(String baseOrderPath) {
        String normalized = baseOrderPath.replace('\\', '/');
        int slash = normalized.lastIndexOf('/');
        String fileName = slash >= 0 ? normalized.substring(slash + 1) : normalized;
        return fileName;
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

    private static final class BMTreeCurve {
        private final Map<Integer, BMTreeNode> nodeById;
        private final BMTreeNode root;
        private final int[] bitLength;
        private final int gridSizeX;
        private final int gridSizeY;

        private BMTreeCurve(Map<Integer, BMTreeNode> nodeById, BMTreeNode root, int[] bitLength) {
            this.nodeById = nodeById;
            this.root = root;
            this.bitLength = bitLength;
            this.gridSizeX = 1 << bitLength[0];
            this.gridSizeY = 1 << bitLength[1];
        }

        private static BMTreeCurve load(String configPath, int[] bitLength) throws IOException {
            List<String> lines = loadLines(configPath);
            if (lines.size() < 3) {
                throw new IOException("Invalid BMTree config: " + configPath);
            }

            String[] headerParts = lines.get(0).trim().split("\\s+");
            int dims = Integer.parseInt(headerParts[0]);
            if (dims != bitLength.length) {
                throw new IOException("BMTree dimension mismatch: " + configPath);
            }
            for (int i = 0; i < dims; i++) {
                int value = Integer.parseInt(headerParts[i + 1]);
                if (value != bitLength[i]) {
                    throw new IOException("BMTree bit length mismatch at dim " + i + ": " + value + " != " + bitLength[i]);
                }
            }

            Map<Integer, BMTreeNode> nodeById = new HashMap<>();
            for (int i = 2; i < lines.size(); i++) {
                String line = lines.get(i).trim();
                if (line.isEmpty()) {
                    continue;
                }
                String[] parts = line.split("\\s+");
                if (parts.length < 7) {
                    throw new IOException("Invalid BMTree node line: " + line);
                }
                BMTreeNode node = new BMTreeNode(
                        Integer.parseInt(parts[0]),
                        Integer.parseInt(parts[1]),
                        Integer.parseInt(parts[2]),
                        Integer.parseInt(parts[3]),
                        Integer.parseInt(parts[4]),
                        Integer.parseInt(parts[5]),
                        Integer.parseInt(parts[6]));
                nodeById.put(node.nodeId, node);
            }

            BMTreeNode root = nodeById.get(0);
            if (root == null) {
                throw new IOException("BMTree root node missing in: " + configPath);
            }
            return new BMTreeCurve(nodeById, root, bitLength.clone());
        }

        private long position(int x, int y) {
            int[] coords = new int[]{x, y};
            int[] usedBits = new int[bitLength.length];
            long result = 0L;
            BMTreeNode current = root;
            while (current != null && current.chosenBit >= 0) {
                int dim = current.chosenBit;
                int bitIndex = bitLength[dim] - 1 - usedBits[dim];
                int bit = bitIndex >= 0 ? ((coords[dim] >>> bitIndex) & 1) : 0;
                result = (result << 1) | bit;
                usedBits[dim] += 1;

                int nextNodeId = bit == 0 ? current.leftChild : current.rightChild;
                if (nextNodeId < 0) {
                    break;
                }
                current = nodeById.get(nextNodeId);
            }
            return result;
        }

        private static List<String> loadLines(String inputPath) throws IOException {
            String normalized = inputPath.trim();
            if (ExperimentPaths.isDistributedPath(normalized) || Files.exists(Paths.get(normalized))) {
                return ExperimentPaths.readAllLines(normalized, StandardCharsets.UTF_8);
            }

            String resourcePath = normalized.replace('\\', '/').replaceAll("^/+", "");
            try (InputStream stream = BMTreeOrderFileBuilder.class.getClassLoader().getResourceAsStream(resourcePath)) {
                if (stream == null) {
                    throw new IOException("BMTree config not found: " + inputPath);
                }
                List<String> lines = new ArrayList<>();
                java.io.BufferedReader reader = new java.io.BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8));
                String line;
                while ((line = reader.readLine()) != null) {
                    lines.add(line);
                }
                return lines;
            }
        }
    }

    private static final class BMTreeNode {
        private final int nodeId;
        private final int parentId;
        private final int depth;
        private final int chosenBit;
        private final int leftChild;
        private final int rightChild;
        private final int child;

        private BMTreeNode(int nodeId, int parentId, int depth, int chosenBit, int leftChild, int rightChild, int child) {
            this.nodeId = nodeId;
            this.parentId = parentId;
            this.depth = depth;
            this.chosenBit = chosenBit;
            this.leftChild = leftChild;
            this.rightChild = rightChild;
            this.child = child;
        }
    }
}
