package utils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Quad信息类
 * 用于存储从RLOrder.json中提取的quad信息以及计算出的新信息
 * 
 * @author hty
 */
public class QuadInfo implements Serializable {
    
    private static final long serialVersionUID = 1L;
    
    // 从原始RLOrder.json中提取（不变）
    public long elementCode;
    public int level;
    public double xmin, ymin, xmax, ymax;
    public int alpha, beta;
    public int originalOrder;
    
    // 子节点列表（从原始quad_code数组中提取）
    public List<Long> childCodes = new ArrayList<>();
    
    // 需要计算的新信息
    public long sfcValue;
    public int newOrder;
    
    /**
     * 获取quad的中心点X坐标
     */
    public double getCenterX() {
        return (xmin + xmax) / 2.0;
    }
    
    /**
     * 获取quad的中心点Y坐标
     */
    public double getCenterY() {
        return (ymin + ymax) / 2.0;
    }
    
    @Override
    public String toString() {
        return String.format("QuadInfo{elementCode=%d, level=%d, center=(%.6f, %.6f), " +
                "originalOrder=%d, sfcValue=%d, newOrder=%d}",
                elementCode, level, getCenterX(), getCenterY(), 
                originalOrder, sfcValue, newOrder);
    }
}
