package experiments.benchmark;

import experiments.benchmark.QueryLoader.QueryWindow;
import experiments.tman.BasicQuery;
import experiments.tman.LetiSpatialQuery;
import experiments.tman.SpatialQuery;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

/**
 * 基准测试执行器 - 运行单个方法的基准测试
 */
public class BenchmarkRunner {

    private final String outputDir;
    
    public BenchmarkRunner(String outputDir) {
        this.outputDir = outputDir;

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
        String[] args = new String[]{tableName, queryString, getOutputPath(method, dataset)};

        // 执行查询
        try {
            BasicQuery query = createQueryExecutor(method);
            if (query != null) {
                long startTime = System.currentTimeMillis();
                query.executeQuery(args);
                long totalTime = System.currentTimeMillis() - startTime;

                // 从CSV结果文件读取统计信息
                loadStatsFromResult(stats, getOutputPath(method, dataset), queries.size());

                System.out.println("Benchmark completed in " + totalTime + " ms");
            }
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
            case BMTREE:
                // TODO: 实现LMSFC和BMT的查询执行器
                System.err.println("Method " + method + " not yet implemented");
                return null;
            default:
                return new SpatialQuery();
        }
    }

    /**
     * 从结果文件加载统计信息
     * 指标与 BasicQuery.saveResult 的 type 列一致：time, logicIndexRanges, rowKeyRanges, candidates, finalSize, vc
     * （兼容旧文件中的 indexRanges 作为逻辑区间行）
     */
    private void loadStatsFromResult(ExperimentStats stats, String resultPath, int queryCount) {
        try {
            List<String> lines = Files.readAllLines(Paths.get(resultPath));
            // 跳过CSV头，解析统计行：type,min,max,avg,mid,per70,per80,per90
            for (int i = 1; i < lines.size(); i++) {
                String line = lines.get(i);
                String[] parts = line.split(",", -1);
                if (parts.length < 4) {
                    continue;
                }
                String type = parts[0].trim();
                long min = Long.parseLong(parts[1].trim());
                long max = Long.parseLong(parts[2].trim());
                long avg = Long.parseLong(parts[3].trim());

                switch (type) {
                    case "time":
                        stats.getLatencyStats().addAggregated(min, max, avg, queryCount);
                        break;
                    case "logicIndexRanges":
                    case "indexRanges":
                        stats.getLogicIndexRangeStats().addAggregated(min, max, avg, queryCount);
                        break;
                    case "rowKeyRanges":
                        stats.getRowKeyRangeStats().addAggregated(min, max, avg, queryCount);
                        break;
                    case "candidates":
                        stats.getCandidatesStats().addAggregated(min, max, avg, queryCount);
                        break;
                    case "finalSize":
                        stats.getFinalResultStats().addAggregated(min, max, avg, queryCount);
                        break;
                    case "vc":
                        stats.getVisitedCellsStats().addAggregated(min, max, avg, queryCount);
                        break;
                    default:
                        break;
                }
            }
        } catch (IOException e) {
            System.err.println("Error loading stats from " + resultPath + ": " + e.getMessage());
        }
    }

    private String getOutputPath(IndexMethod method, DatasetConfig dataset) {
        return String.format("%s/%s_%s.csv", outputDir, method.getShortName(), dataset.toString());
    }
}
