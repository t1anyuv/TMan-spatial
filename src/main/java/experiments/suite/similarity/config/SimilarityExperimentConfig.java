package experiments.suite.similarity.config;

import experiments.common.config.IndexMethod;
import experiments.common.io.ExperimentPaths;
import lombok.Getter;
import lombok.Setter;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

@Getter
@Setter
public class SimilarityExperimentConfig {
    private List<SimilarityExperimentDataset> datasets = new ArrayList<>();
    private List<IndexMethod> methods = new ArrayList<>();
    private List<SimilarityQueryType> queryTypes = new ArrayList<>();
    private List<Integer> topKValues = new ArrayList<>();
    private List<Double> similarityThresholds = new ArrayList<>();
    private List<Integer> distanceFunctions = new ArrayList<>();
    private String outputDir;
    private boolean cleanupTablesAfterRun = true;
    private double topKInitialThreshold = 0.0d;
    private int queryLimit = 0;

    public static SimilarityExperimentConfig load(Path path) throws IOException {
        String raw = ExperimentPaths.readUtf8String(path.toString());
        JSONObject json = new JSONObject(raw);

        SimilarityExperimentConfig config = new SimilarityExperimentConfig();
        config.setOutputDir(json.getString("outputDir"));
        config.setCleanupTablesAfterRun(json.optBoolean("cleanupTablesAfterRun", true));
        config.setTopKInitialThreshold(json.optDouble("topKInitialThreshold", 0.0d));
        config.setQueryLimit(json.optInt("queryLimit", 0));

        JSONArray datasetsJson = json.getJSONArray("datasets");
        for (int i = 0; i < datasetsJson.length(); i++) {
            JSONObject datasetJson = datasetsJson.getJSONObject(i);
            SimilarityExperimentDataset dataset = new SimilarityExperimentDataset();
            dataset.setName(datasetJson.getString("name"));
            dataset.setDataFilePath(datasetJson.getString("dataFilePath"));
            dataset.setQueryWorkloadPath(readQueryWorkloadPath(datasetJson));
            config.datasets.add(dataset);
        }

        JSONArray methodsJson = json.getJSONArray("methods");
        for (int i = 0; i < methodsJson.length(); i++) {
            config.methods.add(parseMethod(methodsJson.getString(i)));
        }

        JSONArray queryTypesJson = json.optJSONArray("queryTypes");
        if (queryTypesJson == null || queryTypesJson.length() == 0) {
            config.queryTypes.add(SimilarityQueryType.TOP_K);
            config.queryTypes.add(SimilarityQueryType.SIMILARITY);
        } else {
            for (int i = 0; i < queryTypesJson.length(); i++) {
                config.queryTypes.add(parseQueryType(queryTypesJson.getString(i)));
            }
        }

        JSONArray topKJson = json.optJSONArray("topKValues");
        if (topKJson != null) {
            for (int i = 0; i < topKJson.length(); i++) {
                config.topKValues.add(topKJson.getInt(i));
            }
        }

        JSONArray thresholdsJson = json.optJSONArray("similarityThresholds");
        if (thresholdsJson != null) {
            for (int i = 0; i < thresholdsJson.length(); i++) {
                config.similarityThresholds.add(thresholdsJson.getDouble(i));
            }
        }

        JSONArray distanceJson = json.optJSONArray("distanceFunctions");
        if (distanceJson == null || distanceJson.length() == 0) {
            config.distanceFunctions.add(0);
            config.distanceFunctions.add(1);
            config.distanceFunctions.add(2);
        } else {
            for (int i = 0; i < distanceJson.length(); i++) {
                Object value = distanceJson.get(i);
                if (value instanceof Number) {
                    config.distanceFunctions.add(((Number) value).intValue());
                } else {
                    config.distanceFunctions.add(parseDistanceFunction(String.valueOf(value)));
                }
            }
        }

        validate(config);
        return config;
    }

    private static void validate(SimilarityExperimentConfig config) {
        if (config.queryTypes.contains(SimilarityQueryType.TOP_K) && config.topKValues.isEmpty()) {
            throw new IllegalArgumentException("topKValues must be provided when queryTypes contains top_k");
        }
        if (config.queryTypes.contains(SimilarityQueryType.SIMILARITY) && config.similarityThresholds.isEmpty()) {
            throw new IllegalArgumentException("similarityThresholds must be provided when queryTypes contains similarity");
        }
        if (config.methods.isEmpty()) {
            throw new IllegalArgumentException("methods must not be empty");
        }
        if (config.datasets.isEmpty()) {
            throw new IllegalArgumentException("datasets must not be empty");
        }
    }

    private static String readQueryWorkloadPath(JSONObject datasetJson) {
        return datasetJson.getString("queryWorkloadPath");
    }

    private static IndexMethod parseMethod(String value) {
        String normalized = value == null ? "" : value.trim().toUpperCase(Locale.ROOT);
        switch (normalized) {
            case "LETI":
                return IndexMethod.LETI;
            case "TSHAPE":
                return IndexMethod.TSHAPE;
            case "XZ_STAR":
            case "XZSTAR":
            case "XZ*":
                return IndexMethod.XZ_STAR;
            default:
                throw new IllegalArgumentException("Unsupported similarity experiment method: " + value);
        }
    }

    private static SimilarityQueryType parseQueryType(String value) {
        String normalized = value == null ? "" : value.trim().toLowerCase(Locale.ROOT);
        switch (normalized) {
            case "top_k":
            case "topk":
                return SimilarityQueryType.TOP_K;
            case "similarity":
            case "sim":
                return SimilarityQueryType.SIMILARITY;
            default:
                throw new IllegalArgumentException("Unsupported similarity query type: " + value);
        }
    }

    private static int parseDistanceFunction(String value) {
        String normalized = value == null ? "" : value.trim().toUpperCase(Locale.ROOT);
        switch (normalized) {
            case "0":
            case "FRECHET":
                return 0;
            case "1":
            case "HAUSDORFF":
                return 1;
            case "2":
            case "DTW":
                return 2;
            default:
                throw new IllegalArgumentException("Unsupported distance function: " + value);
        }
    }
}
