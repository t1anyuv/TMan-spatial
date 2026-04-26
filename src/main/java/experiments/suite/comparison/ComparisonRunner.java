package experiments.suite.comparison;

import experiments.common.config.DatasetConfig;
import experiments.common.execution.PlannerBackedQueryExecutor;
import experiments.common.execution.TableProvisioner;
import experiments.common.execution.TableProvisioner.TableHandle;
import experiments.common.io.QueryLoader;
import experiments.common.model.QueryMetrics;
import experiments.standalone.query.BasicQuery;
import experiments.suite.comparison.config.ComparisonConfig;
import experiments.suite.comparison.config.ComparisonDataset;
import experiments.suite.comparison.model.ExperimentCase;
import experiments.suite.comparison.model.ExperimentResult;
import experiments.suite.comparison.output.SummaryCsvWriter;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

public class ComparisonRunner {
    private final ComparisonConfig config;
    private final TableProvisioner tableProvisioner;
    private final PlannerBackedQueryExecutor queryExecutor = new PlannerBackedQueryExecutor();

    public ComparisonRunner(ComparisonConfig config) {
        this.config = config;
        this.tableProvisioner = new TableProvisioner(config.getOutputDir());
    }

    public void run() throws IOException {
        List<ExperimentResult> results = new ArrayList<>();
        try {
            for (ComparisonDataset dataset : config.getDatasets()) {
                for (String distribution : config.getDistributions()) {
                    for (Integer queryRange : config.getQueryRanges()) {
                        for (experiments.common.config.IndexMethod method : config.getMethods()) {
                            ExperimentCase experimentCase = new ExperimentCase(dataset, distribution, queryRange, method);
                            results.add(runCase(experimentCase));
                        }
                    }
                }
            }
        } finally {
            if (config.isCleanupTablesAfterRun()) {
                tableProvisioner.cleanup();
            }
        }

        Path outputFile = Paths.get(config.getOutputDir(), "comparison_summary.csv");
        SummaryCsvWriter.write(outputFile, results);
        System.out.println("Comparison summary written to: " + outputFile);
    }

    private ExperimentResult runCase(ExperimentCase experimentCase) throws IOException {
        System.out.printf(Locale.ROOT, "%n[Comparison] dataset=%s distribution=%s range=%dm method=%s%n",
                experimentCase.getDataset().getName(),
                experimentCase.getDistribution(),
                experimentCase.getQueryRange(),
                experimentCase.getMethod().getShortName());

        TableHandle tableHandle = tableProvisioner.ensure(experimentCase);
        BasicQuery queryStrategy = PlannerBackedQueryExecutor.createQueryStrategy(tableHandle.getTableConfig());
        List<QueryLoader.QueryWindow> queries = loadQueries(experimentCase);

        if (queries.isEmpty()) {
            throw new IllegalStateException("No queries found for " + experimentCase.getDataset().getName()
                    + " / " + experimentCase.getDistribution()
                    + " / " + experimentCase.getQueryRange());
        }

        queryExecutor.warmup(queries.get(0).toCsvString(), tableHandle.getTableName(), tableHandle.getTableConfig(), queryStrategy);

        long latencySum = 0L;
        long rkiSum = 0L;
        long ctSum = 0L;
        long finalSum = 0L;
        int countedQueries = 0;

        for (int i = 1; i < queries.size(); i++) {
            QueryMetrics metrics = queryExecutor.execute(
                    queries.get(i).toCsvString(),
                    tableHandle.getTableName(),
                    tableHandle.getTableConfig(),
                    queryStrategy);
            latencySum += metrics.getLatencyMs();
            rkiSum += metrics.getRki();
            ctSum += metrics.getCt();
            finalSum += metrics.getFinalCount();
            countedQueries++;
        }

        if (countedQueries == 0) {
            QueryMetrics metrics = queryExecutor.execute(
                    queries.get(0).toCsvString(),
                    tableHandle.getTableName(),
                    tableHandle.getTableConfig(),
                    queryStrategy);
            latencySum += metrics.getLatencyMs();
            rkiSum += metrics.getRki();
            ctSum += metrics.getCt();
            finalSum += metrics.getFinalCount();
            countedQueries = 1;
        }

        return new ExperimentResult(
                experimentCase.getDataset().getName(),
                experimentCase.getDistribution(),
                experimentCase.getQueryRange(),
                experimentCase.getMethod(),
                ((double) latencySum) / countedQueries,
                ((double) rkiSum) / countedQueries,
                ((double) ctSum) / countedQueries,
                ((double) finalSum) / countedQueries,
                countedQueries);
    }

    private List<QueryLoader.QueryWindow> loadQueries(ExperimentCase experimentCase) {
        DatasetConfig datasetConfig = new DatasetConfig(
                experimentCase.getDataset().getName(),
                experimentCase.getDistribution(),
                experimentCase.getQueryRange());
        datasetConfig.setQueryBaseDir(experimentCase.getDataset().getQueryBaseDir());
        QueryLoader loader = new QueryLoader();
        return loader.loadRangeQueries(datasetConfig);
    }
}
