package experiments.benchmark;

import lombok.Getter;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/**
 * 查询加载器 - 从JSON文件加载查询
 * <p>
 * JSON格式示例:
 * [
 *   {"query": "116.042527, 39.947958, 116.066033, 39.965976", "range_meters": 2000},
 *   ...
 * ]
 */
public class QueryLoader {
    
    /**
     * 查询窗口
     */
    @Getter
    public static class QueryWindow {
        private final double minLng;
        private final double minLat;
        private final double maxLng;
        private final double maxLat;
        private final int rangeMeters;
        
        public QueryWindow(double minLng, double minLat, double maxLng, double maxLat, int rangeMeters) {
            this.minLng = minLng;
            this.minLat = minLat;
            this.maxLng = maxLng;
            this.maxLat = maxLat;
            this.rangeMeters = rangeMeters;
        }

        /**
         * 格式化为CSV字符串: minLng,minLat,maxLng,maxLat
         */
        public String toCsvString() {
            return String.format("%.6f,%.6f,%.6f,%.6f", minLng, minLat, maxLng, maxLat);
        }
        
        @Override
        public String toString() {
            return String.format("QueryWindow[%.4f,%.4f,%.4f,%.4f] range=%dm", minLng, minLat, maxLng, maxLat, rangeMeters);
        }
    }
    
    /**
     * 从JSON文件加载查询窗口
     * 使用DatasetConfig中配置的queryBaseDir路径
     */
    public List<QueryWindow> loadQueries(DatasetConfig config) {
        String filePath = config.getQueryFilePath();
        return loadQueriesFromFile(filePath);
    }
    
    /**
     * 从指定JSON文件加载查询
     * JSON格式: [{"query": "x1,y1,x2,y2", "range_meters": 1000}, ...]
     */
    public List<QueryWindow> loadQueriesFromFile(String filePath) {
        List<QueryWindow> queries = new ArrayList<>();
        
        try {
            String content = new String(Files.readAllBytes(Paths.get(filePath)));
            JSONArray jsonArray = new JSONArray(content);
            
            for (int i = 0; i < jsonArray.length(); i++) {
                JSONObject obj = jsonArray.getJSONObject(i);
                String queryStr = obj.getString("query");
                int rangeMeters = obj.getInt("range_meters");
                
                String[] parts = queryStr.split(",");
                if (parts.length == 4) {
                    QueryWindow qw = new QueryWindow(
                        Double.parseDouble(parts[0].trim()),
                        Double.parseDouble(parts[1].trim()),
                        Double.parseDouble(parts[2].trim()),
                        Double.parseDouble(parts[3].trim()),
                        rangeMeters
                    );
                    queries.add(qw);
                }
            }
        } catch (IOException e) {
            System.err.println("Error loading queries from " + filePath + ": " + e.getMessage());
        }
        
        return queries;
    }
    
    /**
     * 从TXT文件加载范围查询
     * 格式: minLng,minLat,maxLng,maxLat
     */
    public List<QueryWindow> loadRangeQueries(DatasetConfig config) {
        String filePath = config.getRangeQueryFilePath();
        List<QueryWindow> queries = new ArrayList<>();
        
        try {
            List<String> lines = Files.readAllLines(Paths.get(filePath));
            for (String line : lines) {
                line = line.trim();
                if (line.isEmpty()) continue;
                
                String[] parts = line.split(",");
                if (parts.length == 4) {
                    QueryWindow qw = new QueryWindow(
                        Double.parseDouble(parts[0].trim()),
                        Double.parseDouble(parts[1].trim()),
                        Double.parseDouble(parts[2].trim()),
                        Double.parseDouble(parts[3].trim()),
                        config.getQueryRange()
                    );
                    queries.add(qw);
                }
            }
        } catch (IOException e) {
            System.err.println("Error loading range queries from " + filePath + ": " + e.getMessage());
        }
        
        return queries;
    }
    
    /**
     * 将查询列表转换为分号分隔的CSV字符串
     */
    public String toQueryString(List<QueryWindow> queries) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < queries.size(); i++) {
            if (i > 0) sb.append(";");
            sb.append(queries.get(i).toCsvString());
        }
        return sb.toString();
    }
}
