package experiments.suite.comparison.model;

import experiments.common.config.IndexMethod;
import experiments.suite.comparison.config.ComparisonDataset;
import lombok.Getter;

@Getter
public class ExperimentCase {
    private final ComparisonDataset dataset;
    private final String distribution;
    private final int queryRange;
    private final IndexMethod method;

    public ExperimentCase(ComparisonDataset dataset, String distribution, int queryRange, IndexMethod method) {
        this.dataset = dataset;
        this.distribution = distribution;
        this.queryRange = queryRange;
        this.method = method;
    }
}
