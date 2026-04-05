package utils;

import com.esri.core.geometry.Envelope;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Getter;

import java.io.*;
import java.util.*;

/**
 * RL Ordering 文件加载器
 * <p>
 * 用于加载和解析 RL Ordering 配置文件（仅支持 JSON 格式），
 * 该文件定义了四叉树节点的排序顺序、父节点信息和自适应划分参数。
 * <p>
 * 主要功能：
 * - 从 classpath 或文件系统加载 RL Ordering JSON 文件
 * - 解析四叉树编码到排序号的映射关系
 * - 解析父节点信息（边界、层级、自适应 alpha/beta）
 * - 计算并缓存最大形状位数（maxShapeBits）
 * - 提供线程安全的静态缓存机制
 * 
 * @author hty
 */
public class LSFCReader {

    /**
     * 静态缓存：资源路径 -> LSFCMapper
     * 使用 volatile + 双重检查锁定确保线程安全
     */
    private static volatile Map<String, LSFCMapper> cache = new HashMap<>();
    private static final Object cacheLock = new Object();

    /**
     * 从 classpath 加载 RL ordering 文件（带缓存）
     * <p>
     * 使用双重检查锁定机制确保线程安全，避免重复加载和解析。
     * 首次加载后结果会被缓存，后续调用直接返回缓存结果。
     * <p>
     * 注意：当前版本仅支持 JSON 格式。
     *
     * @param resourcePath classpath 资源路径（如 "leti/tdrive/uni_order.json"）
     * @return LSFCMapper 包含排序映射、父节点映射和元数据
     * @throws IOException 文件读取异常或资源不存在
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

    /**
     * 清除静态缓存
     * 主要用于测试场景，确保每次测试都重新加载文件。
     * 生产环境不建议调用此方法。
     */
    public static void clearCache() {
        synchronized (cacheLock) {
            cache = new HashMap<>();
        }
    }

    /**
     * 从文件系统加载 RL ordering 文件
     * 注意：此方法不使用缓存，每次调用都会重新解析文件。
     * 如需缓存，请使用 loadFromClasspath 方法。
     * <p>
     * 注意：当前版本仅支持 JSON 格式。
     *
     * @param filePath 文件路径（必须是 .json 文件）
     * @return LSFCMapper 包含排序映射、父节点映射和元数据
     * @throws FileNotFoundException 文件不存在
     * @throws IOException 文件读取异常
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
     * 解析 JSON 格式的 RL ordering 文件
     * <p>
     * JSON 格式示例：
     * <pre>
     * {
     *   "ordering": [
     *     {
     *       "quad_code": [0, 65535, 70997],
     *       "order": 0,
     *       "parent": {
     *         "xmin": -180.0, "ymin": -90.0,
     *         "xmax": 180.0, "ymax": 90.0,
     *         "alpha": 2, "beta": 3,
     *         "level": 0, "elementCode": 0
     *       }
     *     }
     *   ],
     *   "metadata": {
     *     "global_alpha": 2, "global_beta": 2,
     *     "quadtree_max_level": 16,
     *     "active_cells": 1000,
     *     "max_shapes": 100,
     *     "spatial_boundary": { ... }
     *   }
     * }
     * </pre>
     * 
     * 语义说明：
     * - quad_code 数组的第一个元素是父节点（有效节点），后续元素是子节点（无效节点）
     * - 同一数组中的所有节点共享同一个 order 和 parent 信息
     * - parent 字段包含父节点的边界、层级和自适应划分参数（alpha/beta）
     * - 自动计算所有父节点中 alpha * beta 的最大值作为 maxShapeBits
     * 
     * @param root JSON 根节点
     * @return LSFCMapper 包含排序映射、父节点映射和元数据
     */
    private static LSFCMapper parseJsonNode(JsonNode root) {
        Map<Long, Integer> quadCodeOrderMap = new HashMap<>();
        Map<Long, ParentQuad> quadCodeToParentMap = new HashMap<>();
        Set<Long> validParentCodes = new HashSet<>();

        IndexMeta metadata = null;
        JsonNode metaNode = root.get("metadata");
        if (metaNode != null) {
            metadata = new IndexMeta();
            metadata.global_alpha = metaNode.path("global_alpha").asInt(2);
            metadata.global_beta = metaNode.path("global_beta").asInt(2);
            metadata.max_level = metaNode.path("quadtree_max_level").asInt();
            metadata.active_cells = metaNode.path("active_cells").asLong();
            metadata.total_cells = metaNode.path("total_cells").asLong();
            metadata.max_shapes = metaNode.path("max_shapes").asInt();
            metadata.version = metaNode.path("version").asText("");
            metadata.encoding_method = metaNode.path("encoding_method").asText("");
            metadata.generation_timestamp = metaNode.path("generation_timestamp").asText("");
            JsonNode boundaryNode = metaNode.get("spatial_boundary");
            if (boundaryNode != null && !boundaryNode.isMissingNode()) {
                metadata.boundary = new Envelope(
                        boundaryNode.path("xmin").asDouble(),
                        boundaryNode.path("ymin").asDouble(),
                        boundaryNode.path("xmax").asDouble(),
                        boundaryNode.path("ymax").asDouble());
            }
        }

        int maxShapeBits = 0;
        Set<Integer> distinctOrders = new HashSet<>();
        JsonNode orderingNode = root.get("ordering");
        if (orderingNode != null && orderingNode.isArray()) {
            for (JsonNode item : orderingNode) {
                int order = item.get("order").asInt();
                distinctOrders.add(order);
                JsonNode quadCodes = item.get("quad_code");

                long parentCode = quadCodes.get(0).asLong();
                validParentCodes.add(parentCode);

                ParentQuad pq = null;
                JsonNode p = item.get("parent");
                if (p != null) {
                    int pAlpha = p.get("alpha").asInt();
                    int pBeta = p.get("beta").asInt();
                    pq = new ParentQuad(
                            p.get("xmin").asDouble(), p.get("ymin").asDouble(),
                            p.get("xmax").asDouble(), p.get("ymax").asDouble(),
                            pAlpha, pBeta,
                            p.get("level").asInt(), p.get("elementCode").asLong()
                    );
                    quadCodeToParentMap.put(parentCode, pq);

                    int shapeBits = pAlpha * pBeta;
                    if (shapeBits > maxShapeBits) {
                        maxShapeBits = shapeBits;
                    }
                }

                for (JsonNode qc : quadCodes) {
                    long currentCode = qc.asLong();
                    quadCodeOrderMap.put(currentCode, order);
                    if (pq != null) {
                        quadCodeToParentMap.put(currentCode, pq);
                    }
                }
            }
        }

        if (metadata != null) {
            metadata.maxShapeBits = maxShapeBits;
            metadata.orderCount = distinctOrders.size();
        }

        return new LSFCMapper(quadCodeOrderMap, quadCodeToParentMap, validParentCodes, metadata);
    }

    /**
     * 父节点信息类
     * <p>
     * 存储四叉树节点的父节点信息，包括：
     * - 空间边界（xmin, ymin, xmax, ymax）
     * - 自适应划分参数（alpha, beta）
     * - 四叉树层级（level）
     * - 元素编码（elementCode）
     * <p>
     * 与 TShapeEE 的属性保持一致，用于在存储和查询时确定扩展单元格（EE）的边界和划分方式。
     */
    @Getter
    public static class ParentQuad implements Serializable {

        private static final long serialVersionUID = 1L;

        /** 空间边界：最小经度 */
        public final double xmin;
        /** 空间边界：最小纬度 */
        public final double ymin;
        /** 空间边界：最大经度 */
        public final double xmax;
        /** 空间边界：最大纬度 */
        public final double ymax;
        /** 自适应划分参数：X 方向网格数 */
        public final int alpha;
        /** 自适应划分参数：Y 方向网格数 */
        public final int beta;
        /** 四叉树层级 */
        public final int level;
        /** 元素编码（四叉树编码） */
        public final long elementCode;

        /**
         * 构造函数
         * 
         * @param xmin 最小经度
         * @param ymin 最小纬度
         * @param xmax 最大经度
         * @param ymax 最大纬度
         * @param alpha X 方向网格数
         * @param beta Y 方向网格数
         * @param level 四叉树层级
         * @param elementCode 元素编码
         */
        public ParentQuad(double xmin, double ymin, double xmax, double ymax,
                          int alpha, int beta, int level, long elementCode) {
            this.xmin = xmin;
            this.ymin = ymin;
            this.xmax = xmax;
            this.ymax = ymax;
            this.alpha = alpha;
            this.beta = beta;
            this.level = level;
            this.elementCode = elementCode;
        }
    }

    /**
     * RL Ordering 数据映射器
     * <p>
     * 包含从 RL Ordering 文件解析出的所有数据：
     * - 四叉树编码到排序号的映射
     * - 四叉树编码到父节点信息的映射
     * - 有效节点集合（父节点集合）
     * - 元数据信息（全局参数、最大形状位数等）
     */
    @Getter
    public static class LSFCMapper implements Serializable {

        private static final long serialVersionUID = 1L;

        /** 四叉树编码 -> 排序号映射 */
        public final Map<Long, Integer> quadCodeOrder;

        /** 四叉树编码 -> 父节点信息映射 */
        public final Map<Long, ParentQuad> quadCodeParentQuad;

        /** 有效节点集合（父节点集合），不可修改 */
        public final Set<Long> validQuadCodes;

        /** 按 quad code 排序后的有效节点集合，用于范围查询 */
        public final NavigableSet<Long> sortedQuadCodes;

        /** 元数据信息 */
        public final IndexMeta metadata;

        /**
         * 构造函数
         * 
         * @param quadCodeOrder 四叉树编码到排序号的映射
         * @param quadCodeParentQuad 四叉树编码到父节点信息的映射
         * @param metadata 元数据信息
         */
        public LSFCMapper(Map<Long, Integer> quadCodeOrder,
                          Map<Long, ParentQuad> quadCodeParentQuad,
                          Set<Long> validQuadCodes,
                          IndexMeta metadata) {
            this.quadCodeOrder = quadCodeOrder != null ? quadCodeOrder : Collections.emptyMap();
            this.quadCodeParentQuad = quadCodeParentQuad != null ? quadCodeParentQuad : Collections.emptyMap();
            this.metadata = metadata;
            TreeSet<Long> sortedCodes = new TreeSet<>(validQuadCodes != null ? validQuadCodes : Collections.emptySet());
            this.validQuadCodes = Collections.unmodifiableSet(sortedCodes);
            this.sortedQuadCodes = Collections.unmodifiableNavigableSet(sortedCodes);
        }
    }

    /**
     * 索引元数据类
     * <p>
     * 存储 RL Ordering 文件的元数据信息，包括全局参数、空间边界和统计信息。
     */
    public static class IndexMeta implements Serializable {
        private static final long serialVersionUID = 1L;

        /** 最大形状数量 */
        public long max_shapes;
        /** 全局 X 方向网格数 */
        public int global_alpha;
        /** 全局 Y 方向网格数 */
        public int global_beta;
        /** 四叉树最大层级 */
        public int max_level;

        /** 活跃单元格数量 */
        public long active_cells;
        /** 总单元格数量 */
        public long total_cells;

        /** 空间边界 */
        public Envelope boundary;

        /**
         * 自适应划分时的最大形状位数
         * <p>
         * 等于所有 ParentQuad 中 alpha * beta 的最大值。
         * 用于统一编码空间，确保不同父节点的形状编码不会重叠。
         */
        public int maxShapeBits;
        /** Order group count after deduplicating identical order ids */
        public int orderCount;
        /** Metadata version from order file */
        public String version;
        /** Optional encoding method marker */
        public String encoding_method;
        /** Optional generation timestamp */
        public String generation_timestamp;
    }
}


