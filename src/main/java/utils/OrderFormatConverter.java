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
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

public final class OrderFormatConverter {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final DefaultPrettyPrinter PRETTY_PRINTER = createPrettyPrinter();

    private OrderFormatConverter() {
    }

    public enum CoverageMode {
        CONTIGUOUS,
        EXPLICIT
    }

    public static void main(String[] args) throws Exception {
        if (args.length == 0 || "--all".equals(args[0])) {
            convertWorkspaceResources();
            return;
        }

        if (args.length < 2) {
            throw new IllegalArgumentException("Usage: OrderFormatConverter <inputPath> <coverageMode> [outputPath]");
        }

        String inputPath = args[0];
        CoverageMode mode = CoverageMode.valueOf(args[1].trim().toUpperCase());
        String outputPath = args.length >= 3 ? args[2] : inputPath;
        convertFile(inputPath, outputPath, mode);
    }

    public static void convertWorkspaceResources() throws IOException {
        convertDirectory(Paths.get("src/main/resources/leti"), CoverageMode.EXPLICIT);
        convertDirectory(Paths.get("src/main/resources/lmsfc"), CoverageMode.EXPLICIT);
        convertDirectory(Paths.get("src/main/resources/bmtree"), CoverageMode.EXPLICIT);
        Path xzSample = Paths.get("src/main/resources/pruned_xz_order_tdrive_res8_min4.json");
        if (Files.exists(xzSample)) {
            convertFile(xzSample.toString(), xzSample.toString(), CoverageMode.CONTIGUOUS);
        }
    }

    public static void convertDirectory(Path root, CoverageMode defaultMode) throws IOException {
        if (!Files.exists(root)) {
            return;
        }
        Files.walk(root)
                .filter(path -> Files.isRegularFile(path) && path.toString().endsWith(".json"))
                .forEach(path -> {
                    try {
                        CoverageMode mode = detectCoverageMode(path, defaultMode);
                        convertFile(path.toString(), path.toString(), mode);
                    } catch (IOException e) {
                        throw new RuntimeException("Failed to convert order file: " + path, e);
                    }
                });
    }

    public static void convertFile(String inputPath, String outputPath, CoverageMode mode) throws IOException {
        ObjectNode root = loadRoot(inputPath);
        ObjectNode converted = convertOrderRoot(root, mode, inputPath);

        File outputFile = new File(outputPath);
        File parent = outputFile.getParentFile();
        if (parent != null && !parent.exists() && !parent.mkdirs()) {
            throw new IOException("Failed to create output directory: " + parent.getAbsolutePath());
        }

        try (Writer writer = new OutputStreamWriter(Files.newOutputStream(outputFile.toPath()), StandardCharsets.UTF_8)) {
            MAPPER.writer(PRETTY_PRINTER).writeValue(writer, converted);
        }
    }

    public static ObjectNode convertOrderRoot(ObjectNode root, CoverageMode mode, String sourceOrderPath) throws IOException {
        ArrayNode orderingNode = requireOrdering(root, sourceOrderPath);
        int maxLevel = resolveMaxLevel(root, orderingNode);

        List<Entry> entries = extractEntries(orderingNode, maxLevel);
        populateCoverage(entries);

        ObjectNode outputRoot = MAPPER.createObjectNode();
        outputRoot.set("ordering", buildOrdering(entries, mode));
        outputRoot.set("metadata", buildMetadata(root.with("metadata"), entries.size(), mode));
        return outputRoot;
    }

    private static DefaultPrettyPrinter createPrettyPrinter() {
        DefaultPrettyPrinter printer = new DefaultPrettyPrinter();
        DefaultIndenter indenter = new DefaultIndenter("  ", DefaultIndenter.SYS_LF);
        printer.indentObjectsWith(indenter);
        printer.indentArraysWith(indenter);
        return printer;
    }

    private static ObjectNode loadRoot(String inputPath) throws IOException {
        Path path = Paths.get(inputPath);
        if (Files.exists(path)) {
            return (ObjectNode) MAPPER.readTree(path.toFile());
        }
        String resourcePath = inputPath.replace('\\', '/').replaceAll("^/+", "");
        try (InputStream stream = OrderFormatConverter.class.getClassLoader().getResourceAsStream(resourcePath)) {
            if (stream == null) {
                throw new IOException("Order file not found: " + inputPath);
            }
            return (ObjectNode) MAPPER.readTree(stream);
        }
    }

    private static ArrayNode requireOrdering(ObjectNode root, String sourceOrderPath) throws IOException {
        JsonNode ordering = root.get("ordering");
        if (ordering == null || !ordering.isArray()) {
            throw new IOException("Invalid order file, missing ordering array: " + sourceOrderPath);
        }
        return (ArrayNode) ordering;
    }

    private static int resolveMaxLevel(ObjectNode root, ArrayNode ordering) throws IOException {
        JsonNode metadata = root.get("metadata");
        int maxLevel = metadata == null ? 0 : metadata.path("quadtree_max_level").asInt(0);
        if (maxLevel > 0) {
            return maxLevel;
        }

        int derived = 0;
        for (JsonNode item : ordering) {
            JsonNode parent = item.get("parent");
            if (parent != null) {
                derived = Math.max(derived, parent.path("level").asInt(0));
            }
        }
        if (derived == 0 && ordering.size() > 0) {
            throw new IOException("Unable to resolve quadtree_max_level from order metadata");
        }
        return derived;
    }

    private static List<Entry> extractEntries(ArrayNode ordering, int maxLevel) throws IOException {
        List<Entry> entries = new ArrayList<Entry>(ordering.size());
        for (JsonNode item : ordering) {
            JsonNode parentNode = item.get("parent");
            if (parentNode == null || !parentNode.isObject()) {
                throw new IOException("Invalid ordering item, missing parent");
            }

            long elementCode = parentNode.path("elementCode").asLong();
            int level = parentNode.path("level").asInt();
            int order = item.path("order").asInt();

            entries.add(new Entry(
                    order,
                    elementCode,
                    intervalUpperBound(elementCode, level, maxLevel),
                    ((ObjectNode) parentNode).deepCopy()
            ));
        }
        entries.sort(Comparator.comparingLong(entry -> entry.elementCode));
        return entries;
    }

    private static void populateCoverage(List<Entry> entries) {
        ArrayDeque<Entry> stack = new ArrayDeque<Entry>();
        List<Entry> roots = new ArrayList<Entry>();

        for (Entry entry : entries) {
            while (!stack.isEmpty() &&
                    !(entry.elementCode >= stack.peekLast().elementCode && entry.subtreeUpper <= stack.peekLast().subtreeUpper)) {
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

    private static int[] computeSubtreeOrders(Entry node) {
        List<Integer> subtreeOrders = new ArrayList<Integer>();
        subtreeOrders.add(node.order);
        for (Entry child : node.children) {
            int[] childSubtreeOrders = computeSubtreeOrders(child);
            for (int value : childSubtreeOrders) {
                subtreeOrders.add(value);
            }
        }
        subtreeOrders.sort(Integer::compareTo);
        int[] fullSubtreeOrders = subtreeOrders.stream().mapToInt(Integer::intValue).toArray();
        node.effectiveSubtreeOrders = Arrays.copyOfRange(fullSubtreeOrders, 1, fullSubtreeOrders.length);
        node.effectiveSubtreeCount = node.effectiveSubtreeOrders.length;
        return fullSubtreeOrders;
    }

    private static ArrayNode buildOrdering(List<Entry> entries, CoverageMode mode) {
        entries.sort(Comparator.comparingInt(entry -> entry.order));
        ArrayNode ordering = MAPPER.createArrayNode();
        for (Entry entry : entries) {
            ObjectNode item = MAPPER.createObjectNode();
            item.put("order", entry.order);
            ObjectNode parent = entry.parent.deepCopy();
            parent.remove("validChildCount");
            parent.remove("valid_child_count");
            item.set("parent", parent);

            ObjectNode coverage = MAPPER.createObjectNode();
            coverage.put("effective_subtree_count", entry.effectiveSubtreeCount);
            if (mode == CoverageMode.EXPLICIT) {
                ArrayNode orderArray = MAPPER.createArrayNode();
                for (int subtreeOrder : entry.effectiveSubtreeOrders) {
                    orderArray.add(subtreeOrder);
                }
                coverage.set("effective_subtree_orders", orderArray);
            }
            item.set("coverage", coverage);
            ordering.add(item);
        }
        return ordering;
    }

    private static ObjectNode buildMetadata(ObjectNode metadata, int activeCells, CoverageMode mode) {
        ObjectNode result = metadata == null ? MAPPER.createObjectNode() : metadata.deepCopy();
        result.put("active_cells", activeCells);
        if (result.has("total_cells") && result.has("muted_cells")) {
            long totalCells = result.path("total_cells").asLong();
            result.put("muted_cells", totalCells - activeCells);
        }
        result.remove("contiguous_subtree_orders");
        result.put("effective_subtree_contiguous", mode == CoverageMode.CONTIGUOUS);
        return result;
    }

    private static CoverageMode detectCoverageMode(Path path, CoverageMode defaultMode) {
        String normalized = path.toString().replace('\\', '/').toLowerCase();
        if (normalized.contains("xz_pruned") || normalized.contains("pruned_xz")) {
            return CoverageMode.CONTIGUOUS;
        }
        return defaultMode;
    }

    private static long intervalUpperBound(long elementCode, int level, int maxLevel) throws IOException {
        if (level < 0 || maxLevel < level) {
            throw new IOException("Invalid level/maxLevel pair: level=" + level + ", maxLevel=" + maxLevel);
        }
        int exponent = maxLevel - level + 1;
        long power = 1L;
        for (int i = 0; i < exponent; i++) {
            if (power > Long.MAX_VALUE / 4L) {
                throw new IOException("Interval size overflow for maxLevel=" + maxLevel + ", level=" + level);
            }
            power *= 4L;
        }
        return elementCode + ((power - 1L) / 3L);
    }

    private static final class Entry {
        private final int order;
        private final long elementCode;
        private final long subtreeUpper;
        private final ObjectNode parent;
        private final List<Entry> children = new ArrayList<Entry>();
        private int effectiveSubtreeCount;
        private int[] effectiveSubtreeOrders = new int[0];

        private Entry(int order, long elementCode, long subtreeUpper, ObjectNode parent) {
            this.order = order;
            this.elementCode = elementCode;
            this.subtreeUpper = subtreeUpper;
            this.parent = parent;
        }
    }
}
