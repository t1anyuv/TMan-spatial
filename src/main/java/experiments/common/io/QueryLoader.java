package experiments.common.io;

import experiments.common.config.DatasetConfig;
import lombok.Getter;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class QueryLoader {

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

        public String toCsvString() {
            return String.format("%.6f,%.6f,%.6f,%.6f", minLng, minLat, maxLng, maxLat);
        }
    }

    public List<QueryWindow> loadQueries(DatasetConfig config) {
        return loadQueriesFromFile(config.getQueryFilePath());
    }

    public List<QueryWindow> loadQueriesFromFile(String filePath) {
        List<QueryWindow> queries = new ArrayList<>();
        try {
            String content = new String(Files.readAllBytes(Paths.get(filePath)));
            JSONArray jsonArray = new JSONArray(content);
            for (int i = 0; i < jsonArray.length(); i++) {
                JSONObject obj = jsonArray.getJSONObject(i);
                String[] parts = obj.getString("query").split(",");
                int rangeMeters = obj.getInt("range_meters");
                if (parts.length == 4) {
                    queries.add(new QueryWindow(
                            Double.parseDouble(parts[0].trim()),
                            Double.parseDouble(parts[1].trim()),
                            Double.parseDouble(parts[2].trim()),
                            Double.parseDouble(parts[3].trim()),
                            rangeMeters
                    ));
                }
            }
        } catch (IOException e) {
            System.err.println("Error loading queries from " + filePath + ": " + e.getMessage());
        }
        return queries;
    }

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
                    queries.add(new QueryWindow(
                            Double.parseDouble(parts[0].trim()),
                            Double.parseDouble(parts[1].trim()),
                            Double.parseDouble(parts[2].trim()),
                            Double.parseDouble(parts[3].trim()),
                            config.getQueryRange()
                    ));
                }
            }
        } catch (IOException e) {
            System.err.println("Error loading range queries from " + filePath + ": " + e.getMessage());
        }
        return queries;
    }

    public String toQueryString(List<QueryWindow> queries) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < queries.size(); i++) {
            if (i > 0) sb.append(";");
            sb.append(queries.get(i).toCsvString());
        }
        return sb.toString();
    }
}
