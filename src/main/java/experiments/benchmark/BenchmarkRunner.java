package experiments.benchmark;

import experiments.benchmark.config.DatasetConfig;
import experiments.benchmark.config.IndexMethod;
import experiments.benchmark.io.BenchmarkArtifacts;
import experiments.benchmark.io.QueryLoader;
import experiments.benchmark.io.QueryLoader.QueryWindow;
import experiments.benchmark.model.ExperimentStats;
import experiments.tman.BMTreeSpatialQuery;
import experiments.tman.BasicQuery;
import experiments.tman.LMSFCSpatialQuery;
import experiments.tman.LetiSpatialQuery;
import experiments.tman.SpatialQuery;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Locale;

/**
 * 基准测试执行器 - 运行单个方法的基准测试
 */
public class BenchmarkRunner {

    private final String outputDir;
    private final boolean aggregateRangeOutputs;
    
    public BenchmarkRunner(String outputDir) {
        this(outputDir, false);
    }

    public BenchmarkRunner(String outputDir, boolean aggregateRangeOutputs) {
        this.outputDir = outputDir;
        this.aggregateRangeOutputs = aggregateRangeOutputs;

        // 确保输出目录存在
        try {
            Files.createDirectories(Paths.get(outputDir));
        } catch (IOException e) {
            System.err.println("Error creating output directory: " + e.getMessage());
        }
    }

    /**
     * 重复字符串n次
     */
    private static String repeatString() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 60; i++) {
            sb.append("=");
        }
        return sb.toString();
    }

    /**
     * 运行基准测试
     *
     * @param method    索引方法
     * @param dataset   数据集配置
     * @param tableName HBase表名
     */
    public ExperimentStats runBenchmark(IndexMethod method, DatasetConfig dataset, String tableName) {
        System.out.println("\n" + repeatString());
        System.out.println("Running Benchmark: " + method.getShortName() + " on " + dataset);
        System.out.println(repeatString());

        ExperimentStats stats = new ExperimentStats(method, dataset);

        // 加载查询 - 从 ranges 目录加载查询数据
        QueryLoader queryLoader = new QueryLoader();
        List<QueryWindow> queries = queryLoader.loadRangeQueries(dataset);

        if (queries.isEmpty()) {
            System.err.println("No queries loaded for " + dataset);
            return stats;
        }

        System.out.println("Loaded " + queries.size() + " queries");

        // 构建查询执行器
        String queryString = queryLoader.toQueryString(queries);
        String perRangeResultPath = getPerRangeOutputPath(method, dataset);
        String[] args = new String[]{tableName, queryString, perRangeResultPath};

        // 执行查询
        try {
            BasicQuery query = createQueryExecutor(method);
            long startTime = System.currentTimeMillis();
            query.executeQuery(args);
            long totalTime = System.currentTimeMillis() - startTime;

            // 从CSV结果文件读取统计信息
            loadStatsFromResult(stats, perRangeResultPath, queries.size());
            stats.setIndexSizeKb(BenchmarkArtifacts.estimateIndexSizeKb(tableName, "127.0.0.1"));
            if (aggregateRangeOutputs) {
                appendRangeSummary(method, dataset, stats);
            }

            System.out.println("Benchmark completed in " + totalTime + " ms");
        } catch (Exception e) {
            System.err.println("Benchmark failed: " + e.getMessage());
        }

        return stats;
    }

    /**
     * 根据方法创建对应的查询执行器
     */
    private BasicQuery createQueryExecutor(IndexMethod method) {
        switch (method) {
            case LETI:
                // 使用LetiSpatialQuery
                return new LetiSpatialQuery();
            case TSHAPE:
                return new SpatialQuery();
            case XZ_STAR:
                return new SpatialQuery();
            case LMSFC:
                return new LMSFCSpatialQuery();
            case BMTREE:
                return new BMTreeSpatialQuery();
            default:
                return new SpatialQuery();
        }
    }

    /**
     * 从结果文件加载统计信息
     */
    private void loadStatsFromResult(ExperimentStats stats, String resultPath, int queryCount) {
        try {
            List<String> lines = Files.readAllLines(Paths.get(resultPath));
            // 跳过CSV头，兼容两种格式：
            // 1. 直接查询历史格式：type,min,max,avg,mid,per70,per80,per90
            // 2. 简化格式：type,avg
            for (int i = 1; i < lines.size(); i++) {
                String line = lines.get(i);
                String[] parts = line.split(",", -1);
                if (parts.length < 2) {
                    continue;
                }
                String type = parts[0].trim();
                int avgIndex = parts.length >= 4 ? 3 : 1;
                long avg = Long.parseLong(parts[avgIndex].trim());

                switch (type) {
                    case "time":
                        stats.getLatencyStats().addAggregated(avg, avg, avg, queryCount);
                        break;
                    case "logicIndexRanges":
                    case "indexRanges":
                        stats.getLogicIndexRangeStats().addAggregated(avg, avg, avg, queryCount);
                        break;
                    case "quadCodeRanges":
                        stats.getQuadCodeRangeStats().addAggregated(avg, avg, avg, queryCount);
                        break;
                    case "qOrderRanges":
                        stats.getQOrderRangeStats().addAggregated(avg, avg, avg, queryCount);
                        break;
                    case "rowKeyRanges":
                        stats.getRowKeyRangeStats().addAggregated(avg, avg, avg, queryCount);
                        break;
                    case "candidates":
                        stats.getCandidatesStats().addAggregated(avg, avg, avg, queryCount);
                        break;
                    case "finalSize":
                        stats.getFinalResultStats().addAggregated(avg, avg, avg, queryCount);
                        break;
                    case "vc":
                        stats.getVisitedCellsStats().addAggregated(avg, avg, avg, queryCount);
                        break;
                    case "redisAccessCount":
                        stats.getRedisAccessCountStats().addAggregated(avg, avg, avg, queryCount);
                        break;
                    case "redisShapeFilterRateScaled":
                        stats.getRedisShapeFilterRateScaledStats().addAggregated(avg, avg, avg, queryCount);
                        break;
                    default:
                        break;
                }
            }
        } catch (IOException e) {
            System.err.println("Error loading stats from " + resultPath + ": " + e.getMessage());
        }
    }

    private String getPerRangeOutputPath(IndexMethod method, DatasetConfig dataset) {
        return String.format("%s/%s_%s-%s-%dm.csv",
                outputDir,
                method.getShortName(),
                dataset.getDatasetName(),
                dataset.getDistribution(),
                dataset.getQueryRange());
    }

    private String getRangeSummaryOutputPath(IndexMethod method, DatasetConfig dataset) {
        return String.format("%s/%s_%s-%s.csv",
                outputDir,
                method.getShortName(),
                dataset.getDatasetName(),
                dataset.getDistribution());
    }

    private void appendRangeSummary(IndexMethod method, DatasetConfig dataset, ExperimentStats stats) {
        Path path = Paths.get(getRangeSummaryOutputPath(method, dataset));
        boolean exists = Files.exists(path);
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(path.toFile(), true))) {
            if (!exists) {
                writer.write(getRangeSummaryHeader());
                writer.newLine();
            }
            writer.write(toRangeSummaryRow(stats));
            writer.newLine();
        } catch (IOException e) {
            System.err.println("Error appending range summary to " + path + ": " + e.getMessage());
        }
    }

    private String getRangeSummaryHeader() {
        return "Method,Dataset,Distribution,QueryRange_Meters,"
                + "Latency_Avg,LogicalIndexRanges_Avg,QuadCodeRanges_Avg,QOrderRanges_Avg,"
                + "RowKeyRanges_Avg,Candidates_Avg,FinalResultCount_Avg,VisitedCells_Avg,"
                + "RedisAccessCount_Avg,RedisShapeFilterRate_Avg,IndexSize_KB";
    }

    private String toRangeSummaryRow(ExperimentStats stats) {
        return String.format(Locale.US,
                "%s,%s,%s,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d",
                stats.getMethod().getShortName(),
                stats.getDataset().getDatasetName(),
                stats.getDataset().getDistribution(),
                stats.getDataset().getQueryRange(),
                stats.getLatencyStats().getAvg(),
                stats.getLogicIndexRangeStats().getAvg(),
                stats.getQuadCodeRangeStats().getAvg(),
                stats.getQOrderRangeStats().getAvg(),
                stats.getRowKeyRangeStats().getAvg(),
                stats.getCandidatesStats().getAvg(),
                stats.getFinalResultStats().getAvg(),
                stats.getVisitedCellsStats().getAvg(),
                stats.getRedisAccessCountStats().getAvg(),
                stats.getRedisShapeFilterRateScaledStats().getAvg(),
                stats.getIndexSizeKb());
    }
}
