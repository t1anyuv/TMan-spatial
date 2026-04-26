package experiments.suite.similarity.config;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class SimilarityExperimentDataset {
    private String name;
    private String dataFilePath;
    private String queryWorkloadPath;
}
