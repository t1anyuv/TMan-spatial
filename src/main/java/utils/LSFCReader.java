package utils;

import com.esri.core.geometry.Envelope;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Getter;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Reader for the unified effective-node order format.
 * <p>
 * The current format stores only effective nodes. Each ordering item contains:
 * - the effective node's {@code order}
 * - the effective node's {@code parent} geometry/partition metadata
 * - a {@code coverage} block describing the effective descendants covered by the node
 */
public class LSFCReader {

    /** Cached full mapper views keyed by normalized classpath resource path. */
    private static volatile Map<String, LSFCMapper> cache = new HashMap<>();

    /** Cached effective-node indexes keyed by normalized classpath resource path. */
    private static volatile Map<String, EffectiveNodeIndex> effectiveOnlyCache = new HashMap<>();

    private static final Object cacheLock = new Object();

    /**
     * Loads a classpath order file and materializes the full in-memory mapper view.
     */
    public static LSFCMapper loadFromClasspath(String resourcePath) throws IOException {
        String resolvedPath = LetiOrderResolver.resolveClasspathResource(resourcePath);
        String normalizedPath = resolvedPath.startsWith("/") ? resolvedPath : "/" + resolvedPath;

        LSFCMapper cached = cache.get(normalizedPath);
        if (cached != null) {
            return cached;
        }

        synchronized (cacheLock) {
            cached = cache.get(normalizedPath);
            if (cached != null) {
                return cached;
            }

            InputStream inputStream = LSFCReader.class.getResourceAsStream(normalizedPath);
            if (inputStream == null) {
                throw new IOException("Resource not found in classpath: " + resourcePath + " (resolved: " + resolvedPath + ")");
            }

            try {
                ObjectMapper mapper = new ObjectMapper();
                JsonNode root = mapper.readTree(inputStream);
                LSFCMapper result = parseJsonNode(root);

                Map<String, LSFCMapper> newCache = new HashMap<>(cache);
                newCache.put(normalizedPath, result);
                cache = newCache;

                return result;
            } finally {
                inputStream.close();
            }
        }
    }

    /** Clears all static caches. Useful for tests and repeated file regeneration. */
    public static void clearCache() {
        synchronized (cacheLock) {
            cache = new HashMap<>();
            effectiveOnlyCache = new HashMap<>();
        }
    }

    /**
     * Loads only the effective-node index view used by LETI storage-time parent resolution.
     */
    public static EffectiveNodeIndex loadEffectiveOnlyFromClasspath(String resourcePath) throws IOException {
        String resolvedPath = LetiOrderResolver.resolveClasspathResource(resourcePath);
        String normalizedPath = resolvedPath.startsWith("/") ? resolvedPath : "/" + resolvedPath;

        EffectiveNodeIndex cached = effectiveOnlyCache.get(normalizedPath);
        if (cached != null) {
            return cached;
        }

        synchronized (cacheLock) {
            cached = effectiveOnlyCache.get(normalizedPath);
            if (cached != null) {
                return cached;
            }

            InputStream inputStream = LSFCReader.class.getResourceAsStream(normalizedPath);
            if (inputStream == null) {
                throw new IOException("Resource not found in classpath: " + resourcePath + " (resolved: " + resolvedPath + ")");
            }

            try {
                ObjectMapper mapper = new ObjectMapper();
                JsonNode root = mapper.readTree(inputStream);
                EffectiveNodeIndex result = parseEffectiveNodeIndex(root);

                Map<String, EffectiveNodeIndex> newCache = new HashMap<>(effectiveOnlyCache);
                newCache.put(normalizedPath, result);
                effectiveOnlyCache = newCache;

                return result;
            } finally {
                inputStream.close();
            }
        }
    }

    /**
     * Loads an order file from the local filesystem without classpath caching.
     */
    public static LSFCMapper load(String filePath) throws IOException {
        File file = new File(filePath);
        if (!file.exists()) {
            throw new FileNotFoundException("File not found: " + filePath);
        }

        ObjectMapper mapper = new ObjectMapper();
        JsonNode root = mapper.readTree(file);
        return parseJsonNode(root);
    }

    /**
     * Builds the full mapper view from the new effective-node-only format.
     * <p>
     * Since the current format stores only effective nodes, {@code quadCodeOrder}
     * and {@code quadCodeParentQuad} are keyed only by effective node codes.
     */
    private static LSFCMapper parseJsonNode(JsonNode root) {
        Map<Long, Integer> quadCodeOrderMap = new HashMap<>();
        Map<Long, ParentQuad> quadCodeToParentMap = new HashMap<>();
        Set<Long> validParentCodes = new HashSet<>();

        IndexMeta metadata = parseMetadata(root);
        int maxShapeBits = 0;
        Set<Integer> distinctOrders = new HashSet<>();

        JsonNode orderingNode = root.get("ordering");
        boolean sawExplicitSubtreeOrders = false;
        boolean sawSubtreeCountHint = false;
        if (orderingNode != null && orderingNode.isArray()) {
            for (JsonNode item : orderingNode) {
                int order = item.get("order").asInt();
                distinctOrders.add(order);

                ParentQuad parentQuad = parseParentQuad(item);
                if (parentQuad == null) {
                    continue;
                }

                if (parentQuad.effectiveSubtreeOrders.length > 0) {
                    sawExplicitSubtreeOrders = true;
                }
                if (parentQuad.validChildCount >= 0) {
                    sawSubtreeCountHint = true;
                }

                long parentCode = parentQuad.elementCode;
                validParentCodes.add(parentCode);
                quadCodeOrderMap.put(parentCode, order);
                quadCodeToParentMap.put(parentCode, parentQuad);

                int shapeBits = parentQuad.alpha * parentQuad.beta;
                if (shapeBits > maxShapeBits) {
                    maxShapeBits = shapeBits;
                }
            }
        }

        if (metadata != null) {
            metadata.maxShapeBits = maxShapeBits;
            metadata.orderCount = distinctOrders.size();
            if (!metadata.contiguousSubtreeOrdersSpecified && sawSubtreeCountHint && !sawExplicitSubtreeOrders) {
                metadata.contiguousSubtreeOrders = true;
            }
        }

        return new LSFCMapper(quadCodeOrderMap, quadCodeToParentMap, validParentCodes, metadata);
    }

    /**
     * Builds a sorted effective-node index used to map raw quad codes back to
     * the nearest effective ancestor stored in the order file.
     */
    private static EffectiveNodeIndex parseEffectiveNodeIndex(JsonNode root) throws IOException {
        IndexMeta metadata = parseMetadata(root);
        if (metadata == null) {
            throw new IOException("Missing metadata in LETI order file");
        }

        List<EffectiveNodeEntry> entries = new ArrayList<>();
        int maxShapeBits = 0;
        Set<Integer> distinctOrders = new HashSet<>();

        JsonNode orderingNode = root.get("ordering");
        boolean sawExplicitSubtreeOrders = false;
        boolean sawSubtreeCountHint = false;
        if (orderingNode != null && orderingNode.isArray()) {
            for (JsonNode item : orderingNode) {
                int order = item.get("order").asInt();
                distinctOrders.add(order);

                ParentQuad parentQuad = parseParentQuad(item);
                if (parentQuad == null) {
                    continue;
                }

                if (parentQuad.effectiveSubtreeOrders.length > 0) {
                    sawExplicitSubtreeOrders = true;
                }
                if (parentQuad.validChildCount >= 0) {
                    sawSubtreeCountHint = true;
                }

                long elementCode = parentQuad.elementCode;
                long subtreeUpper = intervalUpperBound(elementCode, parentQuad.level, metadata.maxLevel);
                entries.add(new EffectiveNodeEntry(elementCode, subtreeUpper, order, parentQuad));

                int shapeBits = parentQuad.alpha * parentQuad.beta;
                if (shapeBits > maxShapeBits) {
                    maxShapeBits = shapeBits;
                }
            }
        }

        metadata.maxShapeBits = maxShapeBits;
        metadata.orderCount = distinctOrders.size();
        if (!metadata.contiguousSubtreeOrdersSpecified && sawSubtreeCountHint && !sawExplicitSubtreeOrders) {
            metadata.contiguousSubtreeOrders = true;
        }
        entries.sort(Comparator.comparingLong(entry -> entry.elementCode));
        return new EffectiveNodeIndex(Collections.unmodifiableList(entries), metadata);
    }

    /** Parses metadata shared by both full and effective-only views. */
    private static IndexMeta parseMetadata(JsonNode root) {
        JsonNode metaNode = root.get("metadata");
        if (metaNode == null || metaNode.isNull()) {
            return null;
        }

        IndexMeta metadata = new IndexMeta();
        metadata.globalAlpha = readInt(metaNode, 2, "global_alpha");
        metadata.globalBeta = readInt(metaNode, 2, "global_beta");
        metadata.maxLevel = readInt(metaNode, 0, "quadtree_max_level");
        metadata.activeCells = readLong(metaNode, 0L, "active_cells");
        metadata.totalCells = readLong(metaNode, 0L, "total_cells");
        metadata.mutedCells = readLong(metaNode, 0L, "muted_cells");
        metadata.maxShapes = readLong(metaNode, 0L, "max_shape_count");
        metadata.maxPartitionAlpha = readInt(metaNode, 0, "max_partition_alpha");
        metadata.maxPartitionBeta = readInt(metaNode, 0, "max_partition_beta");
        metadata.maxPartition = readInt(metaNode, 0, "max_partition");
        metadata.minTrajs = readInt(metaNode, 0, "min_trajs");
        metadata.version = readText(metaNode, "", "version");
        metadata.encodingMethod = readText(metaNode, "", "order_source");
        metadata.generationTimestamp = readText(metaNode, "", "generation_timestamp");
        metadata.contiguousSubtreeOrdersSpecified = hasAny(metaNode, "effective_subtree_contiguous");
        metadata.contiguousSubtreeOrders = readBoolean(metaNode, false, "effective_subtree_contiguous");

        JsonNode partitionSearchNode = metaNode.get("partition_search");
        if (partitionSearchNode != null && partitionSearchNode.isArray()) {
            metadata.partitionSearch = new int[partitionSearchNode.size()][];
            for (int i = 0; i < partitionSearchNode.size(); i++) {
                JsonNode pairNode = partitionSearchNode.get(i);
                if (pairNode != null && pairNode.isArray() && pairNode.size() >= 2) {
                    metadata.partitionSearch[i] = new int[] {
                            pairNode.get(0).asInt(),
                            pairNode.get(1).asInt()
                    };
                } else {
                    metadata.partitionSearch[i] = new int[0];
                }
            }
        }

        if ((metadata.maxPartitionAlpha == 0 || metadata.maxPartitionBeta == 0) &&
                partitionSearchNode != null && partitionSearchNode.isArray() && partitionSearchNode.size() > 0) {
            JsonNode upperBound = partitionSearchNode.get(partitionSearchNode.size() - 1);
            if (upperBound != null && upperBound.isArray() && upperBound.size() >= 2) {
                metadata.maxPartitionAlpha = upperBound.get(0).asInt();
                metadata.maxPartitionBeta = upperBound.get(1).asInt();
            }
        }

        JsonNode boundaryNode = metaNode.get("spatial_boundary");
        if (boundaryNode != null && !boundaryNode.isMissingNode()) {
            metadata.boundary = new Envelope(
                    readDouble(boundaryNode, 0.0, "xmin"),
                    readDouble(boundaryNode, 0.0, "ymin"),
                    readDouble(boundaryNode, 0.0, "xmax"),
                    readDouble(boundaryNode, 0.0, "ymax"));
        }
        return metadata;
    }

    /** Parses the per-effective-node geometry and coverage hint payload. */
    private static ParentQuad parseParentQuad(JsonNode item) {
        JsonNode parentNode = item.get("parent");
        if (parentNode == null || parentNode.isNull()) {
            return null;
        }

        long elementCode = readLong(parentNode, Long.MIN_VALUE, "element_code");
        if (elementCode == Long.MIN_VALUE) {
            elementCode = readLong(item, Long.MIN_VALUE, "quad_code");
        }
        if (elementCode == Long.MIN_VALUE) {
            return null;
        }

        return new ParentQuad(
                readDouble(parentNode, 0.0, "xmin"),
                readDouble(parentNode, 0.0, "ymin"),
                readDouble(parentNode, 0.0, "xmax"),
                readDouble(parentNode, 0.0, "ymax"),
                readInt(parentNode, 0, "alpha"),
                readInt(parentNode, 0, "beta"),
                readInt(parentNode, 0, "level"),
                elementCode,
                parseEffectiveSubtreeCount(item),
                parseEffectiveSubtreeOrders(item)
        );
    }

    /** Reads the descendant count hint used by contiguous subtree coverage. */
    private static int parseEffectiveSubtreeCount(JsonNode item) {
        JsonNode coverageNode = item.get("coverage");
        if (coverageNode == null || coverageNode.isNull()) {
            return -1;
        }

        JsonNode subtreeNode = coverageNode.get("effective_subtree_count");
        if (subtreeNode == null || subtreeNode.isNull()) {
            return -1;
        }
        return subtreeNode.asInt(-1);
    }

    /** Reads the explicit descendant qOrder list used by non-contiguous coverage. */
    private static int[] parseEffectiveSubtreeOrders(JsonNode item) {
        JsonNode coverageNode = item.get("coverage");
        if (coverageNode == null || coverageNode.isNull()) {
            return new int[0];
        }

        JsonNode ordersNode = coverageNode.get("effective_subtree_orders");
        if (ordersNode == null || !ordersNode.isArray() || ordersNode.size() == 0) {
            return new int[0];
        }

        int[] orders = new int[ordersNode.size()];
        for (int i = 0; i < ordersNode.size(); i++) {
            orders[i] = ordersNode.get(i).asInt();
        }
        return orders;
    }

    private static boolean hasAny(JsonNode node, String... fieldNames) {
        return getFirst(node, fieldNames) != null;
    }

    private static JsonNode getFirst(JsonNode node, String... fieldNames) {
        if (node == null || fieldNames == null) {
            return null;
        }
        for (String fieldName : fieldNames) {
            if (fieldName == null) {
                continue;
            }
            JsonNode value = node.get(fieldName);
            if (value != null && !value.isNull() && !value.isMissingNode()) {
                return value;
            }
        }
        return null;
    }

    private static int readInt(JsonNode node, int defaultValue, String... fieldNames) {
        JsonNode value = getFirst(node, fieldNames);
        return value == null ? defaultValue : value.asInt(defaultValue);
    }

    private static long readLong(JsonNode node, long defaultValue, String... fieldNames) {
        JsonNode value = getFirst(node, fieldNames);
        return value == null ? defaultValue : value.asLong(defaultValue);
    }

    private static double readDouble(JsonNode node, double defaultValue, String... fieldNames) {
        JsonNode value = getFirst(node, fieldNames);
        return value == null ? defaultValue : value.asDouble(defaultValue);
    }

    private static boolean readBoolean(JsonNode node, boolean defaultValue, String... fieldNames) {
        JsonNode value = getFirst(node, fieldNames);
        return value == null ? defaultValue : value.asBoolean(defaultValue);
    }

    private static String readText(JsonNode node, String defaultValue, String... fieldNames) {
        JsonNode value = getFirst(node, fieldNames);
        return value == null ? defaultValue : value.asText(defaultValue);
    }

    /** Computes the closed quad-code interval covered by an effective node's subtree. */
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

    /**
     * Geometry and coverage information attached to one effective node.
     * <p>
     * The coverage fields describe only effective descendants:
     * - {@code validChildCount}: number of descendant effective nodes when subtree qOrders are contiguous
     * - {@code effectiveSubtreeOrders}: explicit descendant qOrders when they are not contiguous
     */
    @Getter
    public static class ParentQuad implements Serializable {

        private static final long serialVersionUID = 1L;

        /** Base cell bounds of the effective node. */
        public final double xmin;
        public final double ymin;
        public final double xmax;
        public final double ymax;

        /** Adaptive partition shape used inside this effective node's EE. */
        public final int alpha;
        public final int beta;

        /** Quadtree level and code of the effective node itself. */
        public final int level;
        public final long elementCode;

        /** Descendant effective-node count hint for contiguous qOrder subtrees. */
        public final int validChildCount;

        /** Explicit descendant qOrders for non-contiguous subtree coverage. */
        public final int[] effectiveSubtreeOrders;

        public ParentQuad(double xmin, double ymin, double xmax, double ymax,
                          int alpha, int beta, int level, long elementCode, int validChildCount,
                          int[] effectiveSubtreeOrders) {
            this.xmin = xmin;
            this.ymin = ymin;
            this.xmax = xmax;
            this.ymax = ymax;
            this.alpha = alpha;
            this.beta = beta;
            this.level = level;
            this.elementCode = elementCode;
            this.validChildCount = validChildCount;
            this.effectiveSubtreeOrders = effectiveSubtreeOrders == null ? new int[0] : effectiveSubtreeOrders;
        }
    }

    /**
     * Full in-memory view of an order file keyed by effective node code.
     * <p>
     * In the unified format only effective nodes are materialized, so these maps
     * intentionally do not contain absorbed raw descendants.
     */
    @Getter
    public static class LSFCMapper implements Serializable {

        private static final long serialVersionUID = 1L;

        /** Effective node code -> qOrder. */
        public final Map<Long, Integer> quadCodeOrder;

        /** Effective node code -> geometry / coverage descriptor. */
        public final Map<Long, ParentQuad> quadCodeParentQuad;

        /** Set of all effective node codes present in the file. */
        public final Set<Long> validQuadCodes;

        /** File-level metadata. */
        public final IndexMeta metadata;

        public LSFCMapper(Map<Long, Integer> quadCodeOrder,
                          Map<Long, ParentQuad> quadCodeParentQuad,
                          Set<Long> validQuadCodes,
                          IndexMeta metadata) {
            this.quadCodeOrder = quadCodeOrder != null ? quadCodeOrder : Collections.emptyMap();
            this.quadCodeParentQuad = quadCodeParentQuad != null ? quadCodeParentQuad : Collections.emptyMap();
            this.metadata = metadata;
            this.validQuadCodes = Collections.unmodifiableSet(
                    validQuadCodes != null ? new HashSet<Long>(validQuadCodes) : Collections.<Long>emptySet());
        }
    }

    /** One effective node plus its subtree interval, used for ancestor lookup. */
    public static class EffectiveNodeEntry implements Serializable {
        private static final long serialVersionUID = 1L;

        public final long elementCode;
        public final long subtreeUpper;
        public final int order;
        public final ParentQuad parentQuad;

        public EffectiveNodeEntry(long elementCode, long subtreeUpper, int order, ParentQuad parentQuad) {
            this.elementCode = elementCode;
            this.subtreeUpper = subtreeUpper;
            this.order = order;
            this.parentQuad = parentQuad;
        }
    }

    /**
     * Sorted effective-node list used to map arbitrary raw quad codes back to the
     * nearest effective ancestor stored in the order file.
     */
    public static class EffectiveNodeIndex implements Serializable {
        private static final long serialVersionUID = 1L;

        public final List<EffectiveNodeEntry> entries;
        public final IndexMeta metadata;

        public EffectiveNodeIndex(List<EffectiveNodeEntry> entries, IndexMeta metadata) {
            this.entries = entries == null ? Collections.emptyList() : entries;
            this.metadata = metadata;
        }

        /**
         * Finds the nearest effective ancestor whose subtree interval still covers
         * the supplied raw quad code.
         */
        public EffectiveNodeEntry resolveNearestEffectiveParent(long rawQuadCode) {
            int left = 0;
            int right = entries.size() - 1;
            int candidate = -1;
            while (left <= right) {
                int mid = (left + right) >>> 1;
                long code = entries.get(mid).elementCode;
                if (code <= rawQuadCode) {
                    candidate = mid;
                    left = mid + 1;
                } else {
                    right = mid - 1;
                }
            }

            for (int i = candidate; i >= 0; i--) {
                EffectiveNodeEntry entry = entries.get(i);
                if (rawQuadCode <= entry.subtreeUpper) {
                    return entry;
                }
            }
            return null;
        }
    }

    /** Metadata block attached to the unified effective-node order file. */
    public static class IndexMeta implements Serializable {
        private static final long serialVersionUID = 1L;

        public long maxShapes;
        public int globalAlpha;
        public int globalBeta;
        public int maxLevel;
        public long activeCells;
        public long totalCells;
        public long mutedCells;
        public Envelope boundary;
        public int maxPartitionAlpha;
        public int maxPartitionBeta;
        public int maxPartition;
        public int minTrajs;
        public int[][] partitionSearch;

        /** Largest {@code alpha * beta} across all effective nodes. */
        public int maxShapeBits;

        /** Number of distinct qOrders stored in the file. */
        public int orderCount;

        public String version;
        public String encodingMethod;
        public String generationTimestamp;

        /** True when each effective node covers one contiguous qOrder interval. */
        public boolean contiguousSubtreeOrders;
        public boolean contiguousSubtreeOrdersSpecified;
    }
}
