package experiments.tman;

import config.TableConfig;
import query.CountPlanner;
import query.QueryPlanner;
import utils.QueryUtils;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.SortedList;
import scala.Tuple2;
import scala.Tuple7;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import static client.Constants.*;

/**
 * 基础查询抽象类，提供查询执行和统计功能
 * <p>
 * 主要功能：
 * - 执行空间范围查询
 * - 收集查询性能统计信息
 * - 保存统计结果到 CSV 文件
 * - 打印格式化的查询结果
 */
public abstract class BasicQuery {

    private static final String CSV_HEADER = "type,min,max,avg,mid,per70,per80,per90\n";
    private static final String CSV_FORMAT = "%s,%d,%d,%d,%d,%d,%d,%d\n";
    private static final String SEPARATOR = "================================================================================";
    private static final String SUB_SEPARATOR = "------------------------------------------------------------";

    /**
     * 将统计结果写入 CSV 文件
     * <p>
     * CSV 列含义：type,min,max,avg,mid,per70,per80,per90
     * <p>
     * 指标说明：
     * - time       : 查询执行时间（毫秒）
     * - logicIndexRanges: 逻辑索引区间数（SFC 上 IndexRange 条数）
     * - rowKeyRanges    : HBase MultiRowRangeFilter 使用的行键区间条数
     * - candidates      : 未经过空间过滤前的候选行键数量（CountPlanner 的结果）
     * - finalSize       : 最终返回的结果数量（经过空间过滤后的结果）
     * - vc              : 访问的单元格数（Visited Cells）
     * <p>
     * 统计值说明：
     * - min   : 所有样本中的最小值
     * - max   : 所有样本中的最大值
     * - avg   : 所有样本的算术平均值
     * - mid   : 中位数（排序后中间位置的值）
     * - per70 : 70 分位数（排序后 70% 位置的值）
     * - per80 : 80 分位数
     * - per90 : 90 分位数
     *
     * @param timeStatistic       查询时间统计列表
     * @param sizeStatistic       最终结果数量统计列表
     * @param candidatesStatistic 候选数量统计列表
     * @param logicIndexRangeSize 逻辑 IndexRange 条数统计列表
     * @param rowKeyRangeSize     行键 RowRange 条数统计列表
     * @param vcStatistic         访问单元格数统计列表
     * @param filePath            输出文件路径
     * @throws IOException 如果写入文件时发生错误
     */
    public static void saveResult(SortedList<Long> timeStatistic,
                                  SortedList<Long> sizeStatistic,
                                  SortedList<Long> candidatesStatistic,
                                  SortedList<Long> logicIndexRangeSize,
                                  SortedList<Long> rowKeyRangeSize,
                                  SortedList<Long> vcStatistic,
                                  String filePath) throws IOException {
        // 确保父目录存在
        Path path = Paths.get(filePath);
        if (path.getParent() != null) {
            Files.createDirectories(path.getParent());
        }

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(filePath))) {
            writer.write(CSV_HEADER);

            // 查询时间统计
            Tuple7<Long, Long, Long, Long, Long, Long, Long> timeStats = getStatistic(timeStatistic);
            writer.write(String.format(CSV_FORMAT, "time",
                    timeStats._1(), timeStats._2(), timeStats._3(), timeStats._4(),
                    timeStats._5(), timeStats._6(), timeStats._7()));

            // 逻辑索引区间数（SFC IndexRange）
            Tuple7<Long, Long, Long, Long, Long, Long, Long> logicStats = getStatistic(logicIndexRangeSize);
            writer.write(String.format(CSV_FORMAT, "logicIndexRanges",
                    logicStats._1(), logicStats._2(), logicStats._3(), logicStats._4(),
                    logicStats._5(), logicStats._6(), logicStats._7()));

            // HBase 扫描行键区间数
            Tuple7<Long, Long, Long, Long, Long, Long, Long> rowKeyStats = getStatistic(rowKeyRangeSize);
            writer.write(String.format(CSV_FORMAT, "rowKeyRanges",
                    rowKeyStats._1(), rowKeyStats._2(), rowKeyStats._3(), rowKeyStats._4(),
                    rowKeyStats._5(), rowKeyStats._6(), rowKeyStats._7()));

            // 未经过空间过滤前的候选数量统计
            Tuple7<Long, Long, Long, Long, Long, Long, Long> candStats = getStatistic(candidatesStatistic);
            writer.write(String.format(CSV_FORMAT, "candidates",
                    candStats._1(), candStats._2(), candStats._3(), candStats._4(),
                    candStats._5(), candStats._6(), candStats._7()));

            // 最终返回结果数量统计
            Tuple7<Long, Long, Long, Long, Long, Long, Long> sizeStats = getStatistic(sizeStatistic);
            writer.write(String.format(CSV_FORMAT, "finalSize",
                    sizeStats._1(), sizeStats._2(), sizeStats._3(), sizeStats._4(),
                    sizeStats._5(), sizeStats._6(), sizeStats._7()));

            // 访问单元格数统计（VC）
            Tuple7<Long, Long, Long, Long, Long, Long, Long> vcStats = getStatistic(vcStatistic);
            writer.write(String.format(CSV_FORMAT, "vc",
                    vcStats._1(), vcStats._2(), vcStats._3(), vcStats._4(),
                    vcStats._5(), vcStats._6(), vcStats._7()));
        }
    }

    /**
     * 对一组已排序的样本做统计，返回统计信息元组
     *
     * @param values 已排序的样本列表（SortedList）
     * @return 统计信息元组 (min, max, avg, median, per70, per80, per90)
     */
    public static Tuple7<Long, Long, Long, Long, Long, Long, Long> getStatistic(SortedList<Long> values) {
        if (values == null || values.isEmpty()) {
            return new Tuple7<>(0L, 0L, 0L, 0L, 0L, 0L, 0L);
        }

        int size = values.size();
        // SortedList 已经排序，直接取首尾元素
        long min = values.get(0);
        long max = values.get(size - 1);

        // 计算平均值
        long sum = 0L;
        for (Long value : values) {
            sum += value;
        }
        long avg = sum / size;

        // 计算分位数索引（确保索引在有效范围内）
        int medianIndex = size / 2;
        int per70Index = Math.max(0, (int) (size * 0.7) - 1);
        int per80Index = Math.max(0, (int) (size * 0.8) - 1);
        int per90Index = Math.max(0, (int) (size * 0.9) - 1);

        long median = values.get(medianIndex);
        long per70 = values.get(per70Index);
        long per80 = values.get(per80Index);
        long per90 = values.get(per90Index);

        return new Tuple7<>(min, max, avg, median, per70, per80, per90);
    }

    public abstract List<FilterBase> getFilters(String condition, TableConfig tableConfig);

    /**
     * 执行查询并收集统计信息
     *
     * @param args 命令行参数：[0] 表名, [1] 查询条件（分号分隔）, [2] 结果文件路径
     * @throws IOException 如果执行查询或写入文件时发生错误
     */
    public void executeQuery(String[] args) throws IOException {
        if (args.length < 3) {
            throw new IllegalArgumentException("Usage: <table_name> <query_conditions> <result_path>");
        }

        String table = args[0];
        String[] queryConditions = args[1].split(";");
        String resultPath = args[2];

        QueryUtils queryUtils = new QueryUtils();
        TableConfig tableConfig = queryUtils.getTableConfig(table + META_TABLE);

        printHeader("Query Execution Started");
        System.out.printf("Table Name    : %s%n", table);
        System.out.printf("Total Queries : %d%n", queryConditions.length);
        System.out.printf("Result Path   : %s%n", resultPath);
        System.out.println(SEPARATOR + "\n");

        try (QueryPlanner queryPlanner = new QueryPlanner(null, tableConfig, table);
             CountPlanner countPlanner = new CountPlanner(null, tableConfig, table)) {

            // 初始化统计列表
            SortedList<Long> timeStatistic = new SortedList<>(Long::compare);
            SortedList<Long> sizeStatistic = new SortedList<>(Long::compare);
            SortedList<Long> logicIndexRangeStatistic = new SortedList<>(Long::compare);
            SortedList<Long> rowKeyRangeStatistic = new SortedList<>(Long::compare);
            SortedList<Long> candidatesStatistic = new SortedList<>(Long::compare);
            SortedList<Long> vcStatistic = new SortedList<>(Long::compare);

            // 预热查询（使用第一个查询条件）
            if (queryConditions.length > 0) {
                System.out.println(SUB_SEPARATOR);
                System.out.println("Warmup Query (not included in statistics)");
                System.out.println(SUB_SEPARATOR);
                
                List<FilterBase> warmupFilters = getFilters(queryConditions[0], tableConfig);
                Tuple2<Integer, ResultScanner> warmupResult = queryPlanner.executeByFilter(warmupFilters, tableConfig, table);
                
                if (warmupResult != null && warmupResult._2 != null) {
                    long warmupSize = countResults(warmupResult._2);
                    System.out.printf("Warmup completed: %d results%n", warmupSize);
                } else {
                    System.out.println("Warmup completed: no results");
                }
                System.out.println();
            }

            // 执行所有查询并收集统计信息
            for (int i = 0; i < queryConditions.length; i++) {
                String condition = queryConditions[i];
                
                printQueryHeader(i + 1, queryConditions.length, condition);

                try {
                    List<FilterBase> filters = getFilters(condition, tableConfig);
                    long startTimeNs = System.nanoTime();
                    Tuple2<Integer, ResultScanner> result = queryPlanner.executeByFilter(filters, tableConfig, table);

                    if (result != null && result._2() != null) {
                        // 统计最终结果数量
                        long finalSize = countResults(result._2());
                        long queryTime = (System.nanoTime() - startTimeNs) / 1_000_000L;

                        // 使用 countPlanner 统计候选数量
                        Tuple2<Integer, ResultScanner> countResult = countPlanner.executeByFilter(filters, tableConfig, table);
                        long candidates = (countResult != null && countResult._2() != null)
                                ? countResults(countResult._2()) : 0;

                        // 获取访问的单元格数与行键区间数
                        int visitedCells = queryPlanner.getLastVisitedCells();
                        int rowKeyRanges = queryPlanner.getLastRowRangeCount();

                        // 记录统计信息
                        timeStatistic.add(queryTime);
                        logicIndexRangeStatistic.add(Long.valueOf(result._1()));
                        rowKeyRangeStatistic.add((long) rowKeyRanges);
                        sizeStatistic.add(finalSize);
                        candidatesStatistic.add(candidates);
                        vcStatistic.add((long) visitedCells);

                        // 打印查询结果
                        printQueryResult(queryTime, result._1(), rowKeyRanges, candidates, finalSize, visitedCells);
                    } else {
                        System.out.println("⚠ Query returned null result");
                    }
                } catch (Exception e) {
                    System.err.printf("✗ Query %d failed: %s%n", i + 1, e.getMessage());
                }

                System.out.println();
            }

            // 保存统计结果
            printHeader("Saving Statistics");
            saveResult(timeStatistic, sizeStatistic, candidatesStatistic, logicIndexRangeStatistic,
                    rowKeyRangeStatistic, vcStatistic, resultPath);
            System.out.printf("✓ Statistics saved to: %s%n", resultPath);
            System.out.println();

            // 打印汇总统计信息
            printSummaryStatistics(timeStatistic, sizeStatistic, candidatesStatistic, logicIndexRangeStatistic,
                    rowKeyRangeStatistic, vcStatistic);

        } catch (Exception e) {
            System.err.println("✗ Query execution failed: " + e.getMessage());
            throw new IOException("Query execution failed", e);
        }
    }

    /**
     * 打印汇总统计信息
     */
    private void printSummaryStatistics(SortedList<Long> timeStatistic,
                                        SortedList<Long> sizeStatistic,
                                        SortedList<Long> candidatesStatistic,
                                        SortedList<Long> logicIndexRangeStatistic,
                                        SortedList<Long> rowKeyRangeStatistic,
                                        SortedList<Long> vcStatistic) {
        printHeader("Summary Statistics");

        if (!timeStatistic.isEmpty()) {
            Tuple7<Long, Long, Long, Long, Long, Long, Long> stats = getStatistic(timeStatistic);
            printStatLine("Query Time (ms)", stats);
        }

        if (!logicIndexRangeStatistic.isEmpty()) {
            Tuple7<Long, Long, Long, Long, Long, Long, Long> stats = getStatistic(logicIndexRangeStatistic);
            printStatLine("Logic index ranges (SFC)", stats);
        }

        if (!rowKeyRangeStatistic.isEmpty()) {
            Tuple7<Long, Long, Long, Long, Long, Long, Long> stats = getStatistic(rowKeyRangeStatistic);
            printStatLine("Row-key scan ranges", stats);
        }

        if (!candidatesStatistic.isEmpty()) {
            Tuple7<Long, Long, Long, Long, Long, Long, Long> stats = getStatistic(candidatesStatistic);
            printStatLine("Candidates", stats);
        }

        if (!sizeStatistic.isEmpty()) {
            Tuple7<Long, Long, Long, Long, Long, Long, Long> stats = getStatistic(sizeStatistic);
            printStatLine("Final Results", stats);
        }

        if (!vcStatistic.isEmpty()) {
            Tuple7<Long, Long, Long, Long, Long, Long, Long> stats = getStatistic(vcStatistic);
            printStatLine("Visited Cells", stats);
        }

        System.out.println(SEPARATOR + "\n");
    }

    /**
     * 打印标题
     */
    private void printHeader(String title) {
        System.out.println(SEPARATOR);
        System.out.println(title);
        System.out.println(SEPARATOR);
    }

    /**
     * 打印查询标题
     */
    private void printQueryHeader(int current, int total, String condition) {
        System.out.println(SUB_SEPARATOR);
        System.out.printf("Query %d / %d%n", current, total);
        System.out.println(SUB_SEPARATOR);
        System.out.printf("Condition: %s%n", condition);
    }

    /**
     * 打印查询结果
     */
    private void printQueryResult(long queryTime, int logicIndexRanges, int rowKeyRanges, long candidates, long finalSize,
                                  int visitedCells) {
        System.out.printf("✓ Query Time      : %d ms%n", queryTime);
        System.out.printf("  Logic ranges    : %d (SFC IndexRange)%n", logicIndexRanges);
        System.out.printf("  Row-key ranges  : %d (HBase scan)%n", rowKeyRanges);
        System.out.printf("  Candidates      : %d (before spatial filter)%n", candidates);
        System.out.printf("  Final Results   : %d (after spatial filter)%n", finalSize);
        System.out.printf("  Visited Cells   : %d%n", visitedCells);
    }

    /**
     * 打印统计行
     */
    private void printStatLine(String label, Tuple7<Long, Long, Long, Long, Long, Long, Long> stats) {
        System.out.printf("%-18s: Min=%6d, Max=%6d, Avg=%6d, Median=%6d, P70=%6d, P80=%6d, P90=%6d%n",
                label, stats._1(), stats._2(), stats._3(), stats._4(), 
                stats._5(), stats._6(), stats._7());
    }

    /**
     * 统计结果数量
     */
    private long countResults(ResultScanner resultScanner) {
        if (resultScanner == null) {
            return 0;
        }

        long count = 0;
        try {
            for (Result ignored : resultScanner) {
                count++;
            }
        } catch (Exception e) {
            System.err.println("⚠ Error while counting results: " + e.getMessage());
        } finally {
            try {
                resultScanner.close();
            } catch (Exception e) {
                System.err.println("⚠ Error closing result scanner: " + e.getMessage());
            }
        }
        return count;
    }

    /**
     * 打印查询结果并返回结果数量（已弃用，使用 countResults 代替）
     *
     * @param resultScanner 结果扫描器
     * @param label         标签，用于区分不同的指标（如 "size" 或 "candidates"）
     * @return 结果数量
     * @deprecated 使用 {@link #countResults(ResultScanner)} 代替
     */
    @Deprecated
    public long printResult(ResultScanner resultScanner, String label) {
        return countResults(resultScanner);
    }

    /**
     * 保存查询结果的 RowKey 到文件
     * <p>
     * 将每个结果的 RowKey 以二进制字符串格式写入文件，每行一个。
     *
     * @param scanner    结果扫描器
     * @param idFilePath ID 存储路径
     * @return 保存的记录数量
     */
    public long saveResultIds(ResultScanner scanner, String idFilePath) {
        if (scanner == null) {
            System.out.println("⚠ Scanner is null, no IDs to save");
            return 0;
        }

        long count = 0;
        Path path = Paths.get(idFilePath);
        
        try {
            // 确保父目录存在
            if (path.getParent() != null) {
                Files.createDirectories(path.getParent());
            }

            try (BufferedWriter writer = new BufferedWriter(new FileWriter(idFilePath, false))) {
                for (Result result : scanner) {
                    byte[] rowKey = result.getRow();
                    if (rowKey != null) {
                        writer.write(Bytes.toStringBinary(rowKey));
                        writer.newLine();
                        count++;

                        if (count % 10000 == 0) {
                            System.out.printf("  Saved %d IDs...%n", count);
                        }
                    }
                }
            }
            
            System.out.printf("✓ Saved %d IDs to: %s%n", count, idFilePath);
        } catch (IOException e) {
            System.err.printf("✗ Error saving IDs: %s%n", e.getMessage());
        } finally {
            try {
                scanner.close();
            } catch (Exception e) {
                System.err.println("⚠ Error closing scanner: " + e.getMessage());
            }
        }

        return count;
    }
}
