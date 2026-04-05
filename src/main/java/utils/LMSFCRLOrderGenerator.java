package utils;

import com.esri.core.geometry.MultiPoint;
import com.esri.core.geometry.Point;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import index.LMSFCIndex;
import scala.Tuple2;
import scala.Tuple3;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;

/**
 * LMSFC RLOrder生成器
 * 
 * 将LMSFC的编码方式应用到LETI的层级结构上，通过重新生成标准 order 配置文件，
 * 在保持LETI框架不变的情况下，引入LMSFC的空间填充曲线优势。
 * 
 * 核心流程：
 * 1. 提取quad信息 - 从现有的标准 order 文件中读取1690个quad的elementCode和空间边界信息
 * 2. 计算LMSFC SFC值 - 使用LMSFC的theta配置为每个quad计算空间填充曲线值
 * 3. 按SFC值排序 - 根据计算出的SFC值对所有quad重新排序
 * 4. 分配新的order值 - 为排序后的quad分配新的连续order编号（0-1689）
 * 5. 生成新配置文件 - 输出lmsfc/tdrive/uni_order.json，保持elementCode不变，只修改order值
 * 
 * @author hty
 */
public class LMSFCRLOrderGenerator {
    
    /**
     * 主程序入口
     */
    public static void main(String[] args) {
        try {
            // 配置参数
            String originalRLOrderPath = "leti/tdrive/uni_order.json";
            String outputPath = "src/main/resources/lmsfc/tdrive/uni_order.json";
            String thetaConfig = "0, 1, 2, 3, 5, 7, 8, 9, 10, 11, 18, 19, 21, 24, 25, 26, 28, 29, 31, 36, 4, 6, 12, 13, 14, 15, 16, 17, 20, 22, 23, 27, 30, 32, 33, 34, 35, 37, 38, 39";
            int resolution = 20; 
            
            System.out.println("========================================");
            System.out.println("LMSFC RLOrder生成器");
            System.out.println("========================================");
            System.out.println("输入文件: " + originalRLOrderPath);
            System.out.println("输出文件: " + outputPath);
            System.out.println("Theta配置: " + thetaConfig);
            System.out.println("分辨率: " + resolution + " (网格大小: " + (1 << resolution) + ")");
            System.out.println("========================================\n");
            
            // 步骤1：提取quad信息
            System.out.println("[步骤1] 提取quad信息...");
            List<QuadInfo> quads = extractQuadInfo(originalRLOrderPath);
            System.out.println("提取完成，共 " + quads.size() + " 个quad\n");
            
            // 步骤2：计算SFC值
            System.out.println("[步骤2] 计算LMSFC SFC值...");
            calculateSFCValues(quads, thetaConfig, resolution);
            System.out.println("计算完成\n");
            
            // 步骤3：排序
            System.out.println("[步骤3] 按SFC值排序...");
            sortQuads(quads);
            System.out.println("排序完成\n");
            
            // 步骤4：分配order
            System.out.println("[步骤4] 分配新的order值...");
            Map<Long, Integer> newOrders = assignNewOrders(quads);
            System.out.println("分配完成，共 " + newOrders.size() + " 个映射\n");
            
            // 步骤5：生成JSON
            System.out.println("[步骤5] 生成 lmsfc/tdrive/uni_order.json ...");
            generateRLOrderLMSFC(quads, outputPath, thetaConfig);
            System.out.println("生成完成\n");
            
            // 验证
            System.out.println("[验证] 检查生成结果...");
            verifyResult(originalRLOrderPath, outputPath);
            System.out.println("验证通过\n");
            
            // 统计信息
            printStatistics(quads);
            
            System.out.println("========================================");
            System.out.println("处理完成！");
            System.out.println("========================================");
            
        } catch (Exception e) {
            System.err.println("错误: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
    
    /**
     * 步骤1：从原始 LETI order 文件中提取 quad 信息
     * 提取父节点及其对应的所有子节点
     */
    public static List<QuadInfo> extractQuadInfo(String rlOrderPath) throws IOException {
        // 首先通过LSFCMapper获取映射关系
        LSFCReader.LSFCMapper mapper = LSFCReader.loadFromClasspath(rlOrderPath);
        
        // 直接解析JSON以获取完整的quad_code数组
        ObjectMapper jsonMapper = new ObjectMapper();
        InputStream inputStream = LSFCReader.class.getResourceAsStream("/" + rlOrderPath);
        if (inputStream == null) {
            throw new IOException("Resource not found: " + rlOrderPath);
        }
        
        JsonNode root = jsonMapper.readTree(inputStream);
        JsonNode orderingNode = root.get("ordering");
        
        List<QuadInfo> allQuads = new ArrayList<>();
        
        for (JsonNode item : orderingNode) {
            JsonNode quadCodes = item.get("quad_code");
            if (quadCodes == null || quadCodes.size() == 0) {
                continue;
            }
            
            // 第一个元素是父节点
            long parentCode = quadCodes.get(0).asLong();
            
            // 从parent字段提取信息
            JsonNode parentNode = item.get("parent");
            if (parentNode == null) {
                System.err.println("警告: elementCode " + parentCode + " 缺少parent信息，跳过");
                continue;
            }
            
            QuadInfo quad = new QuadInfo();
            quad.elementCode = parentCode;
            quad.level = parentNode.get("level").asInt();
            quad.xmin = parentNode.get("xmin").asDouble();
            quad.ymin = parentNode.get("ymin").asDouble();
            quad.xmax = parentNode.get("xmax").asDouble();
            quad.ymax = parentNode.get("ymax").asDouble();
            quad.alpha = parentNode.get("alpha").asInt();
            quad.beta = parentNode.get("beta").asInt();
            quad.originalOrder = item.get("order").asInt();
            
            // 提取所有子节点（从第二个元素开始）
            for (int i = 1; i < quadCodes.size(); i++) {
                quad.childCodes.add(quadCodes.get(i).asLong());
            }
            
            allQuads.add(quad);
        }
        
        int totalNodes = allQuads.stream().mapToInt(q -> 1 + q.childCodes.size()).sum();
        System.out.println("  - 提取了 " + allQuads.size() + " 个父节点quad");
        System.out.println("  - 包含 " + (totalNodes - allQuads.size()) + " 个子节点");
        System.out.println("  - 总节点数: " + totalNodes);
        
        return allQuads;
    }
    
    /**
     * 步骤2：计算每个quad的LMSFC SFC值
     */
    public static void calculateSFCValues(
        List<QuadInfo> quads,
        String thetaConfig,
        int resolution
    ) {
        // 获取全局边界
        double globalXmin = quads.stream().mapToDouble(q -> q.xmin).min().orElse(0);
        double globalXmax = quads.stream().mapToDouble(q -> q.xmax).max().orElse(0);
        double globalYmin = quads.stream().mapToDouble(q -> q.ymin).min().orElse(0);
        double globalYmax = quads.stream().mapToDouble(q -> q.ymax).max().orElse(0);
        
        System.out.println("  - 全局边界: X[" + globalXmin + ", " + globalXmax + "], " +
                          "Y[" + globalYmin + ", " + globalYmax + "]");
        
        // 初始化LMSFC索引
        LMSFCIndex lmsfcIndex = LMSFCIndex.apply(
            (short) resolution,
            new Tuple2<>(globalXmin, globalXmax),
            new Tuple2<>(globalYmin, globalYmax),
            thetaConfig
        );
        
        System.out.println("  - LMSFC索引初始化完成");
        
        // 计算每个quad的SFC值
        int count = 0;
        for (QuadInfo quad : quads) {
            // 使用quad的中心点
            double centerX = quad.getCenterX();
            double centerY = quad.getCenterY();
            
            // 创建点的几何对象
            Point centerPoint = new Point(centerX, centerY);
            MultiPoint geo = new MultiPoint();
            geo.add(centerPoint);
            
            // 计算SFC值
            Tuple3<Object, Object, Object> result = lmsfcIndex.index(geo, false);
            quad.sfcValue = (long) result._2();  // minSFC
            
            count++;
            if (count % 500 == 0) {
                System.out.println("  - 已处理 " + count + "/" + quads.size() + " 个quad");
            }
        }
        
        System.out.println("  - 已处理 " + count + "/" + quads.size() + " 个quad");
    }
    
    /**
     * 步骤3：按SFC值排序
     */
    public static void sortQuads(List<QuadInfo> quads) {
        Collections.sort(quads, new Comparator<QuadInfo>() {
            @Override
            public int compare(QuadInfo q1, QuadInfo q2) {
                // 主排序：按SFC值
                int sfcCompare = Long.compare(q1.sfcValue, q2.sfcValue);
                if (sfcCompare != 0) return sfcCompare;
                
                // 次排序：按层级（SFC相同时，低层级优先）
                int levelCompare = Integer.compare(q1.level, q2.level);
                if (levelCompare != 0) return levelCompare;
                
                // 三级排序：按elementCode（保证稳定排序）
                return Long.compare(q1.elementCode, q2.elementCode);
            }
        });
        
        System.out.println("  - 排序完成");
    }
    
    /**
     * 步骤4：分配新的order值
     */
    public static Map<Long, Integer> assignNewOrders(List<QuadInfo> quads) {
        Map<Long, Integer> elementCodeToNewOrder = new HashMap<>();
        
        for (int i = 0; i < quads.size(); i++) {
            QuadInfo quad = quads.get(i);
            quad.newOrder = i;  // 新的order就是排序后的索引
            elementCodeToNewOrder.put(quad.elementCode, i);
        }
        
        System.out.println("  - 分配了 " + elementCodeToNewOrder.size() + " 个新order");
        
        return elementCodeToNewOrder;
    }
    
    /**
     * 步骤5：生成 lmsfc/tdrive/uni_order.json 文件
     */
    public static void generateRLOrderLMSFC(
        List<QuadInfo> quads,
        String outputPath,
        String thetaConfig
    ) throws IOException {
        // 加载原始配置以获取metadata信息
        LSFCReader.LSFCMapper originalMapper = LSFCReader.loadFromClasspath("leti/tdrive/uni_order.json");
        LSFCReader.IndexMeta originalMeta = originalMapper.getMetadata();
        
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode root = mapper.createObjectNode();
        
        // 创建ordering数组
        ArrayNode orderingArray = mapper.createArrayNode();
        
        for (QuadInfo quad : quads) {
            ObjectNode item = mapper.createObjectNode();
            
            // quad_code数组（包含父节点和所有子节点）
            ArrayNode quadCodeArray = mapper.createArrayNode();
            quadCodeArray.add(quad.elementCode);  // 父节点
            for (Long childCode : quad.childCodes) {
                quadCodeArray.add(childCode);  // 子节点
            }
            item.set("quad_code", quadCodeArray);
            
            // order（新的order值）
            item.put("order", quad.newOrder);
            
            // parent信息（保持不变）
            ObjectNode parent = mapper.createObjectNode();
            parent.put("alpha", quad.alpha);
            parent.put("beta", quad.beta);
            parent.put("level", quad.level);
            parent.put("elementCode", quad.elementCode);
            parent.put("xmin", quad.xmin);
            parent.put("ymin", quad.ymin);
            parent.put("xmax", quad.xmax);
            parent.put("ymax", quad.ymax);
            item.set("parent", parent);
            
            orderingArray.add(item);
        }
        
        root.set("ordering", orderingArray);
        
        // 创建metadata
        ObjectNode metadata = mapper.createObjectNode();
        
        // 计算统计信息
        int maxShapeBits = quads.stream()
            .mapToInt(q -> q.alpha * q.beta)
            .max()
            .orElse(0);
        
        // 从原始metadata中获取值，如果不存在则使用默认值
        int globalAlpha = originalMeta != null ? originalMeta.global_alpha : 3;
        int globalBeta = originalMeta != null ? originalMeta.global_beta : 3;
        int maxLevel = originalMeta != null ? originalMeta.max_level : 8;
        long totalCells = originalMeta != null ? originalMeta.total_cells : 87381;
        
        metadata.put("total_cells", totalCells);
        metadata.put("active_cells", quads.size());
        metadata.put("muted_cells", totalCells - quads.size());
        
        // 添加spatial_boundary（从原始metadata或计算得出）
        ObjectNode spatialBoundary = mapper.createObjectNode();
        if (originalMeta != null && originalMeta.boundary != null) {
            spatialBoundary.put("xmin", originalMeta.boundary.getXMin());
            spatialBoundary.put("ymin", originalMeta.boundary.getYMin());
            spatialBoundary.put("xmax", originalMeta.boundary.getXMax());
            spatialBoundary.put("ymax", originalMeta.boundary.getYMax());
        } else {
            // 从quads计算边界
            double xmin = quads.stream().mapToDouble(q -> q.xmin).min().orElse(0);
            double ymin = quads.stream().mapToDouble(q -> q.ymin).min().orElse(0);
            double xmax = quads.stream().mapToDouble(q -> q.xmax).max().orElse(0);
            double ymax = quads.stream().mapToDouble(q -> q.ymax).max().orElse(0);
            spatialBoundary.put("xmin", xmin);
            spatialBoundary.put("ymin", ymin);
            spatialBoundary.put("xmax", xmax);
            spatialBoundary.put("ymax", ymax);
        }
        metadata.set("spatial_boundary", spatialBoundary);
        
        metadata.put("max_shapes", quads.size());
        metadata.put("quadtree_max_level", maxLevel);
        metadata.put("global_alpha", globalAlpha);
        metadata.put("global_beta", globalBeta);
        metadata.put("version", "2.0-LMSFC");
        metadata.put("encoding_method", "LMSFC");
        metadata.put("theta_config", thetaConfig);
        metadata.put("maxShapeBits", maxShapeBits);
        
        root.set("metadata", metadata);
        
        // 写入文件
        File outputFile = new File(outputPath);
        outputFile.getParentFile().mkdirs();
        
        try (FileWriter writer = new FileWriter(outputFile)) {
            String json = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(root);
            writer.write(json);
        }
        
        System.out.println("  - 文件已写入: " + outputPath);
        System.out.println("  - 文件大小: " + outputFile.length() + " 字节");
    }
    
    /**
     * 验证生成结果
     */
    public static void verifyResult(String originalPath, String newPath) throws IOException {
        LSFCReader.LSFCMapper originalMapper = LSFCReader.loadFromClasspath(originalPath);
        
        // 清除缓存以重新加载新文件
        LSFCReader.clearCache();
        LSFCReader.LSFCMapper newMapper = LSFCReader.load(newPath);
        
        // 提取原始的所有节点集合（父节点+子节点）
        Set<Long> originalAllCodes = originalMapper.getValidQuadCodes();
        
        // 新文件的所有节点集合
        Set<Long> newAllCodes = newMapper.getValidQuadCodes();
        
        // 检查所有节点集合是否一致（包括父节点和子节点）
        if (!originalAllCodes.equals(newAllCodes)) {
            throw new RuntimeException("验证失败: elementCode集合不一致。" +
                "原始: " + originalAllCodes.size() + " 个, " +
                "新生成: " + newAllCodes.size() + " 个");
        }
        
        System.out.println("  - elementCode集合一致: " + originalAllCodes.size() + " 个（包括父节点和子节点）");
        
        // 检查所有节点都有order映射
        for (Long code : originalAllCodes) {
            if (!newMapper.getQuadCodeOrder().containsKey(code)) {
                throw new RuntimeException("验证失败: elementCode " + code + " 缺少新的order");
            }
        }
        
        System.out.println("  - 所有节点都有order映射");
        
        // 提取父节点集合用于验证order连续性
        Set<Long> originalParentCodes = new HashSet<>();
        for (Long elementCode : originalMapper.getValidQuadCodes()) {
            LSFCReader.ParentQuad parent = originalMapper.getQuadCodeParentQuad().get(elementCode);
            if (parent != null && parent.getElementCode() == elementCode) {
                originalParentCodes.add(elementCode);
            }
        }
        
        // 检查父节点的order值是否连续且唯一
        Set<Integer> orderSet = new HashSet<>();
        for (Long code : originalParentCodes) {
            Integer order = newMapper.getQuadCodeOrder().get(code);
            if (orderSet.contains(order)) {
                throw new RuntimeException("验证失败: order值 " + order + " 重复");
            }
            orderSet.add(order);
        }
        
        int maxOrder = orderSet.stream().max(Integer::compare).orElse(-1);
        int minOrder = orderSet.stream().min(Integer::compare).orElse(-1);
        if (minOrder != 0 || maxOrder != originalParentCodes.size() - 1) {
            throw new RuntimeException("验证失败: order值不连续。期望 [0, " + 
                (originalParentCodes.size() - 1) + "], 实际 [" + minOrder + ", " + maxOrder + "]");
        }
        
        System.out.println("  - 父节点order值连续且唯一: [0, " + maxOrder + "]");
    }
    
    /**
     * 打印统计信息
     */
    public static void printStatistics(List<QuadInfo> quads) {
        System.out.println("========================================");
        System.out.println("统计信息");
        System.out.println("========================================");
        
        // order变化统计
        int changedCount = 0;
        int maxOrderChange = 0;
        long totalOrderChange = 0;
        
        for (QuadInfo quad : quads) {
            int change = Math.abs(quad.newOrder - quad.originalOrder);
            if (change > 0) {
                changedCount++;
                totalOrderChange += change;
                maxOrderChange = Math.max(maxOrderChange, change);
            }
        }
        
        System.out.println("总quad数: " + quads.size());
        System.out.println("order发生变化的quad数: " + changedCount + 
                          " (" + String.format("%.2f", changedCount * 100.0 / quads.size()) + "%)");
        System.out.println("最大order变化: " + maxOrderChange);
        if (changedCount > 0) {
            System.out.println("平均order变化: " + 
                              String.format("%.2f", totalOrderChange * 1.0 / changedCount));
        }
        
        // 层级分布
        Map<Integer, Integer> levelCount = new HashMap<>();
        for (QuadInfo quad : quads) {
            levelCount.put(quad.level, levelCount.getOrDefault(quad.level, 0) + 1);
        }
        
        System.out.println("\n层级分布:");
        levelCount.entrySet().stream()
            .sorted(Map.Entry.comparingByKey())
            .forEach(e -> System.out.println("  Level " + e.getKey() + ": " + e.getValue() + " quads"));
        
        // SFC值范围
        long minSFC = quads.stream().mapToLong(q -> q.sfcValue).min().orElse(0);
        long maxSFC = quads.stream().mapToLong(q -> q.sfcValue).max().orElse(0);
        
        System.out.println("\nSFC值范围: [" + minSFC + ", " + maxSFC + "]");
        
        // 示例：前5个和后5个quad
        System.out.println("\n前5个quad (按新order):");
        for (int i = 0; i < Math.min(5, quads.size()); i++) {
            QuadInfo q = quads.get(i);
            System.out.println(String.format("  [%d] elementCode=%d, level=%d, " +
                    "originalOrder=%d, sfcValue=%d",
                    q.newOrder, q.elementCode, q.level, q.originalOrder, q.sfcValue));
        }
        
        System.out.println("\n后5个quad (按新order):");
        for (int i = Math.max(0, quads.size() - 5); i < quads.size(); i++) {
            QuadInfo q = quads.get(i);
            System.out.println(String.format("  [%d] elementCode=%d, level=%d, " +
                    "originalOrder=%d, sfcValue=%d",
                    q.newOrder, q.elementCode, q.level, q.originalOrder, q.sfcValue));
        }
    }
}
