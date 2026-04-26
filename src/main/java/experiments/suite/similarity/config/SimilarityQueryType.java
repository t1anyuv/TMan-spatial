package experiments.suite.similarity.config;

import lombok.Getter;

@Getter
public enum SimilarityQueryType {
    TOP_K("top_k"),
    SIMILARITY("similarity");

    private final String configValue;

    SimilarityQueryType(String configValue) {
        this.configValue = configValue;
    }

}
