package experiments.suite.comparison.config;

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
public class ComparisonConfig {
    private List<ComparisonDataset> datasets = new ArrayList<>();
    private List<String> distributions = new ArrayList<>();
    private List<Integer> queryRanges = new ArrayList<>();
    private List<IndexMethod> methods = new ArrayList<>();
    private String outputDir;
    private boolean cleanupTablesAfterRun = true;

    public static ComparisonConfig load(Path path) throws IOException {
        String raw = ExperimentPaths.readUtf8String(path.toString());
        JSONObject json = new JSONObject(raw);

        ComparisonConfig config = new ComparisonConfig();
        config.setOutputDir(json.getString("outputDir"));
        config.setCleanupTablesAfterRun(json.optBoolean("cleanupTablesAfterRun", true));

        JSONArray datasetsJson = json.getJSONArray("datasets");
        for (int i = 0; i < datasetsJson.length(); i++) {
            JSONObject datasetJson = datasetsJson.getJSONObject(i);
            ComparisonDataset dataset = new ComparisonDataset();
            dataset.setName(datasetJson.getString("name"));
            dataset.setQueryBaseDir(datasetJson.getString("queryBaseDir"));
            dataset.setDataFilePath(readDataFilePath(datasetJson));
            dataset.setResolution(readOptionalInt(datasetJson, "resolution"));
            dataset.setMinTraj(readOptionalInt(datasetJson, "minTraj"));
            dataset.setAlpha(readOptionalInt(datasetJson, "alpha"));
            dataset.setBeta(readOptionalInt(datasetJson, "beta"));
            dataset.setLetiAlpha(readOptionalInt(datasetJson, "letiAlpha"));
            dataset.setLetiBeta(readOptionalInt(datasetJson, "letiBeta"));
            dataset.setNodes(readOptionalInt(datasetJson, "nodes"));
            dataset.setShards(readOptionalInt(datasetJson, "shards"));
            dataset.setQueryType(datasetJson.optString("queryType", null));
            dataset.setThetaConfig(datasetJson.optString("thetaConfig", null));
            dataset.setThetaConfigPath(datasetJson.optString("thetaConfigPath", null));
            dataset.setBmtreeConfigPath(datasetJson.optString("bmtreeConfigPath", null));
            dataset.setBmtreeBitLength(datasetJson.optString("bmtreeBitLength", null));
            config.datasets.add(dataset);
        }

        JSONArray distributionsJson = json.getJSONArray("distributions");
        for (int i = 0; i < distributionsJson.length(); i++) {
            config.distributions.add(distributionsJson.getString(i));
        }

        JSONArray rangesJson = json.getJSONArray("queryRanges");
        for (int i = 0; i < rangesJson.length(); i++) {
            config.queryRanges.add(rangesJson.getInt(i));
        }

        JSONArray methodsJson = json.getJSONArray("methods");
        for (int i = 0; i < methodsJson.length(); i++) {
            config.methods.add(parseMethod(methodsJson.getString(i)));
        }

        return config;
    }

    private static String readDataFilePath(JSONObject datasetJson) {
        if (!datasetJson.has("dataFilePath")) {
            throw new IllegalArgumentException("Dataset config must contain dataFilePath");
        }
        return datasetJson.getString("dataFilePath");
    }

    private static Integer readOptionalInt(JSONObject json, String key) {
        return json.has(key) ? json.getInt(key) : null;
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
                return IndexMethod.XZ_STAR;
            case "LMSFC":
                return IndexMethod.LMSFC;
            case "BMT":
            case "BMTREE":
                return IndexMethod.BMTREE;
            default:
                throw new IllegalArgumentException("Unsupported method: " + value);
        }
    }
}
