package utils;

import java.util.ArrayList;
import java.util.List;

/**
 * TSP (Traveling Salesman Problem) 贪心算法实现
 * 用于对形状编码进行排序优化，减少存储和查询时的随机访问
 * 
 * @author hehuajun
 */
public class TSPGreedy {
    
    /**
     * 对形状列表进行编码排序
     * 
     * @param shapes 形状编码列表
     * @param tspEncoding TSP 编码类型：0=不编码，1=贪心算法，2=遗传算法，3=不编码
     * @return 排序后的索引列表
     */
    public static List<Integer> encodeShapes(List<Long> shapes, int tspEncoding) {
        if (shapes == null || shapes.isEmpty()) {
            return new ArrayList<>();
        }
        
        // 如果不使用 TSP 编码，直接返回原始顺序
        if (tspEncoding == 0 || tspEncoding == 3) {
            List<Integer> orders = new ArrayList<>(shapes.size());
            for (int i = 0; i < shapes.size(); i++) {
                orders.add(i);
            }
            return orders;
        }
        
        // 使用贪心算法进行 TSP 排序
        if (tspEncoding == 1) {
            return tspGreedy(shapes);
        }
        
        // 使用遗传算法进行 TSP 排序（暂未实现，使用贪心算法代替）
        if (tspEncoding == 2) {
            System.out.println("[TSPGreedy] 警告: 遗传算法暂未实现，使用贪心算法代替");
            return tspGreedy(shapes);
        }
        
        // 默认返回原始顺序
        List<Integer> orders = new ArrayList<>(shapes.size());
        for (int i = 0; i < shapes.size(); i++) {
            orders.add(i);
        }
        return orders;
    }
    
    /**
     * TSP 贪心算法实现
     * 基于汉明距离（Hamming Distance）的贪心策略
     * 
     * @param shapes 形状编码列表
     * @return 排序后的索引列表
     */
    private static List<Integer> tspGreedy(List<Long> shapes) {
        int n = shapes.size();
        if (n == 0) {
            return new ArrayList<>();
        }
        
        if (n == 1) {
            List<Integer> result = new ArrayList<>();
            result.add(0);
            return result;
        }
        
        // 计算汉明距离矩阵
        int[][] distances = new int[n][n];
        for (int i = 0; i < n; i++) {
            for (int j = i + 1; j < n; j++) {
                int dist = hammingDistance(shapes.get(i), shapes.get(j));
                distances[i][j] = dist;
                distances[j][i] = dist;
            }
        }
        
        // 贪心算法：从第一个节点开始，每次选择距离最近的未访问节点
        boolean[] visited = new boolean[n];
        List<Integer> tour = new ArrayList<>(n);
        
        int current = 0;
        tour.add(current);
        visited[current] = true;
        
        for (int i = 1; i < n; i++) {
            int nearest = -1;
            int minDist = Integer.MAX_VALUE;
            
            for (int j = 0; j < n; j++) {
                if (!visited[j] && distances[current][j] < minDist) {
                    minDist = distances[current][j];
                    nearest = j;
                }
            }
            
            if (nearest != -1) {
                tour.add(nearest);
                visited[nearest] = true;
                current = nearest;
            }
        }
        
        return tour;
    }
    
    /**
     * 计算两个 long 值的汉明距离（不同位的数量）
     * 
     * @param a 第一个值
     * @param b 第二个值
     * @return 汉明距离
     */
    private static int hammingDistance(long a, long b) {
        long xor = a ^ b;
        return Long.bitCount(xor);
    }
}
