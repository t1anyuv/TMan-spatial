package experiments.validate;

import com.esri.core.geometry.*;
import config.TableConfig;
import filter.SpatialFilter;
import query.QueryPlanner;
import utils.QueryUtils;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

import static client.Constants.*;

/**
 * SpatialQuery 正确性验证程序
 * 以文件扫描为 baseline，与 SpatialQuery 的查询结果进行比较
 *
 */
public class SpatialQueryValidate {

    /**
     * 扫描目录下的所有轨迹文件（part-*****格式），找出与查询窗口相交的轨迹
     *
     * @param directoryPath 轨迹文件目录路径
     * @param queryWindow   查询窗口（Envelope）
     * @return 与查询窗口相交的轨迹集合（oid-tid）
     */
    public static Set<TrajectoryInfo> scanTrajectoriesFromFiles(String directoryPath, Envelope queryWindow) throws IOException {
        Set<TrajectoryInfo> intersectingTrajectories = new HashSet<>();
        OperatorImportFromWkt importerWKT = (OperatorImportFromWkt)
                OperatorFactoryLocal.getInstance().getOperator(Operator.Type.ImportFromWkt);

        SpatialReference spatialRef = SpatialReference.create(4326);

        Path dir = Paths.get(directoryPath);
        if (!Files.exists(dir) || !Files.isDirectory(dir)) {
            throw new IOException("目录不存在或不是有效目录: " + directoryPath);
        }

        System.out.println("开始扫描目录: " + directoryPath);
        final int[] totalCount = {0};
        final int[] errorCount = {0};

        // 递归扫描所有文件，特别是 part-***** 格式的文件
        Files.walk(dir)
                .filter(Files::isRegularFile)
                .filter(path -> path.getFileName().toString().startsWith("part-") ||
                        path.getFileName().toString().matches("part-\\d+"))
                .forEach(entry -> {
                    try (BufferedReader reader = Files.newBufferedReader(entry)) {
                        String line;
                        while ((line = reader.readLine()) != null) {
                            synchronized (intersectingTrajectories) {
                                totalCount[0]++;
                            }
                            if (line.trim().isEmpty()) {
                                continue;
                            }

                            try {
                                // 解析轨迹行：oid-tid-MULTIPOINT Z(...)
                                // 格式：7146-7146_663-MULTIPOINT Z((116.44154 39.95047 1202372999000), ...))
                                String[] parts = line.split("-", 3);
                                if (parts.length < 3) {
                                    synchronized (intersectingTrajectories) {
                                        errorCount[0]++;
                                    }
                                    continue;
                                }

                                String oid = parts[0].trim();
                                String tid = parts[1].trim();
                                String wkt = parts[2].trim();

                                // 解析 WKT 为 MultiPoint
                                MultiPoint multiPoint = (MultiPoint) importerWKT.execute(
                                        0, Geometry.Type.MultiPoint, wkt, null);

                                // 判断是否与查询窗口相交
                                if (OperatorIntersects.local().execute(queryWindow, multiPoint, spatialRef, null)) {
                                    synchronized (intersectingTrajectories) {
                                        intersectingTrajectories.add(new TrajectoryInfo(oid, tid));
                                    }
                                }
                            } catch (Exception e) {
                                synchronized (intersectingTrajectories) {
                                    errorCount[0]++;
                                }
                                // 静默处理错误行，继续处理下一行
                            }
                        }
                    } catch (IOException e) {
                        System.err.println("读取文件失败: " + entry + ", 错误: " + e.getMessage());
                    }
                });

        System.out.printf("扫描完成: 总行数=%d, 错误行数=%d, 相交轨迹数=%d%n",
                totalCount[0], errorCount[0], intersectingTrajectories.size());

        return intersectingTrajectories;
    }

    /**
     * 解析查询条件字符串为 Envelope
     * 格式: "xmin, ymin, xmax, ymax"
     */
    public static Envelope parseQueryWindow(String queryCondition) {
        String[] parts = queryCondition.trim().split(",");
        if (parts.length != 4) {
            throw new IllegalArgumentException("查询条件格式错误，应为: xmin, ymin, xmax, ymax");
        }

        double xmin = Double.parseDouble(parts[0].trim());
        double ymin = Double.parseDouble(parts[1].trim());
        double xmax = Double.parseDouble(parts[2].trim());
        double ymax = Double.parseDouble(parts[3].trim());

        return new Envelope(xmin, ymin, xmax, ymax);
    }

    /**
     * 从 SpatialQuery 获取查询结果
     *
     * @param tableName      表名
     * @param queryCondition 查询条件（格式: "xmin, ymin, xmax, ymax"）
     * @return 查询结果信息（轨迹集合 + 统计信息）
     */
    public static QueryResult getQueryResults(String tableName, String queryCondition) throws IOException {
        return new SpatialQueryValidate().getQueryResultsInternal(tableName, queryCondition);
    }

    public QueryResult getQueryResultsInternal(String tableName, String queryCondition) throws IOException {
        Set<TrajectoryInfo> results = new HashSet<>();
        long totalRows = 0;
        long missingIdRows = 0;
        long duplicateRows = 0;

        QueryUtils queryUtils = new QueryUtils();
        TableConfig tableConfig = queryUtils.getTableConfig(tableName + META_TABLE);

        try (QueryPlanner queryPlanner = new QueryPlanner(null, tableConfig, tableName)) {
            // 构建查询过滤器
            String[] xy = queryCondition.trim().split(",");
            Envelope env = new Envelope(
                    Double.parseDouble(xy[0].trim()),
                    Double.parseDouble(xy[1].trim()),
                    Double.parseDouble(xy[2].trim()),
                    Double.parseDouble(xy[3].trim()));

            SpatialFilter spatialFilter = createSpatialFilter(env, tableConfig);

            List<FilterBase> filters = new ArrayList<>();
            filters.add(spatialFilter);

            // 执行查询
            scala.Tuple2<Integer, ResultScanner> queryResult = queryPlanner.executeByFilter(
                    filters, tableConfig, tableName);

            if (queryResult != null && queryResult._2() != null) {
                ResultScanner resultScanner = queryResult._2();

                for (Result result : resultScanner) {
                    totalRows++;
                    try {
                        // 提取 oid 和 tid
                        byte[] oidBytes = result.getValue(
                                Bytes.toBytes(DEFAULT_CF),
                                Bytes.toBytes(O_ID));
                        byte[] tidBytes = result.getValue(
                                Bytes.toBytes(DEFAULT_CF),
                                Bytes.toBytes(T_ID));

                        if (oidBytes != null && tidBytes != null) {
                            String oid;
                            // oid 可能存储为 Long 或 String，尝试两种方式
                            try {
                                oid = String.valueOf(Bytes.toLong(oidBytes));
                            } catch (Exception e) {
                                oid = Bytes.toString(oidBytes);
                            }
                            String tid = Bytes.toString(tidBytes);
                            boolean added = results.add(new TrajectoryInfo(oid, tid));
                            if (!added) {
                                duplicateRows++;
                            }
                        } else {
                            missingIdRows++;
                        }
                    } catch (Exception e) {
                        // 忽略无法解析的结果
                        System.err.println("解析查询结果失败: " + e.getMessage());
                    }
                }

                resultScanner.close();
            }
        }

        System.out.printf("查询完成: 找到 %d 个轨迹%n", results.size());
        System.out.printf("查询结果统计: HBase行数=%d, 缺失oid/tid=%d, 重复oid/tid=%d%n",
                totalRows, missingIdRows, duplicateRows);

        return new QueryResult(results, totalRows, missingIdRows, duplicateRows);
    }

    protected SpatialFilter createSpatialFilter(Envelope env, TableConfig tableConfig) {
        return new SpatialFilter(
                GeometryEngine.geometryToWkt(env, 0),
                tableConfig.getCompressType().toString());
    }

    /**
     * 比较两个轨迹集合
     */
    public static ValidationResult compareResults(Set<TrajectoryInfo> baseline, Set<TrajectoryInfo> queryResults) {
        ValidationResult result = new ValidationResult();
        result.baselineCount = baseline.size();
        result.queryCount = queryResults.size();

        // 找出只在 baseline 中的轨迹（漏检）
        Set<TrajectoryInfo> missing = new HashSet<>(baseline);
        missing.removeAll(queryResults);
        result.missingTrajectories = missing;

        // 找出只在 queryResults 中的轨迹（误检）
        Set<TrajectoryInfo> extra = new HashSet<>(queryResults);
        extra.removeAll(baseline);
        result.extraTrajectories = extra;

        // 找出共同的结果（正确检测）
        Set<TrajectoryInfo> correct = new HashSet<>(baseline);
        correct.retainAll(queryResults);
        result.correctTrajectories = correct;

        result.isValid = missing.isEmpty() && extra.isEmpty();

        return result;
    }

    /**
     * 将轨迹集合写入文件
     */
    private static void writeTrajectorySet(Path filePath, Set<TrajectoryInfo> trajectories) throws IOException {
        Files.createDirectories(filePath.getParent());
        List<String> lines = new ArrayList<>(trajectories.size());
        for (TrajectoryInfo traj : trajectories) {
            lines.add(traj.toString());
        }
        lines.sort(Comparator.naturalOrder());
        Files.write(filePath, lines);
    }

    /**
     * 将验证结果写入指定目录（按查询条件编号区分）
     */
    private static void dumpValidationResult(Path outputDir,
                                             int queryIndex,
                                             String queryCondition,
                                             Set<TrajectoryInfo> baseline,
                                             QueryResult queryResults,
                                             ValidationResult validation) throws IOException {
        String prefix = String.format("query_%02d_", queryIndex + 1);
        writeTrajectorySet(outputDir.resolve(prefix + "baseline.txt"), baseline);
        writeTrajectorySet(outputDir.resolve(prefix + "query.txt"), queryResults.results);
        writeTrajectorySet(outputDir.resolve(prefix + "missing.txt"), validation.missingTrajectories);
        writeTrajectorySet(outputDir.resolve(prefix + "extra.txt"), validation.extraTrajectories);
        writeTrajectorySet(outputDir.resolve(prefix + "correct.txt"), validation.correctTrajectories);

        List<String> summary = new ArrayList<>();
        summary.add("queryCondition=" + queryCondition);
        summary.add("baselineCount=" + validation.baselineCount);
        summary.add("queryCount=" + validation.queryCount);
        summary.add("hbaseRowCount=" + queryResults.totalRows);
        summary.add("missingIdRows=" + queryResults.missingIdRows);
        summary.add("duplicateRows=" + queryResults.duplicateRows);
        summary.add("correctCount=" + validation.correctTrajectories.size());
        summary.add("missingCount=" + validation.missingTrajectories.size());
        summary.add("extraCount=" + validation.extraTrajectories.size());
        Files.write(outputDir.resolve(prefix + "summary.txt"), summary);
    }

    /**
     * 将分号分隔的查询条件解析为列表
     */
    private static List<String> parseQueryConditions(String rawQueryConditions) {
        List<String> conditions = new ArrayList<>();
        if (rawQueryConditions == null || rawQueryConditions.trim().isEmpty()) {
            return conditions;
        }
        String[] parts = rawQueryConditions.split(";");
        for (String part : parts) {
            String trimmed = part.trim();
            if (!trimmed.isEmpty()) {
                conditions.add(trimmed);
            }
        }
        return conditions;
    }

    /**
     * 重复字符串（兼容 Java 8）
     */
    private static String repeatString(String str, int count) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < count; i++) {
            sb.append(str);
        }
        return sb.toString();
    }

    /**
     * 主函数
     * 参数:
     * args[0] - 轨迹文件目录路径
     * args[1] - 表名
     * args[2] - 查询条件（格式: "xmin, ymin, xmax, ymax"）
     */
    public static void main(String[] args) throws IOException {
        runValidate(args, new SpatialQueryValidate());
    }

    protected static void runValidate(String[] args, SpatialQueryValidate validator) throws IOException {
        if (args.length < 3) {
            System.out.println("用法: java SpatialQueryValidator <轨迹目录> <表名> <查询条件>");
            System.out.println("  轨迹目录: 包含 part-***** 格式轨迹文件的目录路径");
            System.out.println("  表名: HBase 表名");
            System.out.println("  查询条件: 可为单个窗口 \"xmin, ymin, xmax, ymax\"");
            System.out.println("             或多个窗口，用分号分隔：\"x1, y1, x2, y2; x3, y3, x4, y4; ...\"");
            System.out.println("  [可选] 输出目录: 用于写出 baseline/query/missing/extra 列表");
            System.out.println("\n示例:");
            System.out.println("  java SpatialQueryValidator /path/to/trajectories Tman_tdrive_r14 \"115.9549, 40.4434, 115.9599, 40.4484\" /tmp/validate_out");
            System.exit(1);
        }

        String directoryPath = args[0];
        String tableName = args[1];
        String queryConditionRaw = args[2];
        String outputDir = args.length >= 4 ? args[3] : null;

        List<String> queryConditions = parseQueryConditions(queryConditionRaw);
        if (queryConditions.isEmpty()) {
            System.out.println("错误: 未解析到有效的查询条件，请检查输入格式。");
            System.exit(1);
        }

        System.out.println(repeatString("=", 80));
        System.out.println("SpatialQuery 正确性验证");
        System.out.println(repeatString("=", 80));
        System.out.println("轨迹目录: " + directoryPath);
        System.out.println("表名: " + tableName);
        System.out.println("查询条件: " + queryConditionRaw);
        System.out.println(repeatString("=", 80));

        boolean hasFailure = false;
        for (int i = 0; i < queryConditions.size(); i++) {
            String queryCondition = queryConditions.get(i);
            System.out.println("\n" + repeatString("-", 80));
            System.out.printf("处理查询 %d/%d: %s%n", i + 1, queryConditions.size(), queryCondition);

            // 解析查询窗口
            Envelope queryWindow = parseQueryWindow(queryCondition);
            System.out.printf("查询窗口: xmin=%.6f, ymin=%.6f, xmax=%.6f, ymax=%.6f%n",
                    queryWindow.getXMin(), queryWindow.getYMin(),
                    queryWindow.getXMax(), queryWindow.getYMax());

            // 步骤1: 文件扫描（baseline）
            System.out.println("\n步骤1: 文件扫描 (Baseline)...");
            long startTime = System.currentTimeMillis();
            Set<TrajectoryInfo> baselineResults = scanTrajectoriesFromFiles(directoryPath, queryWindow);
            long scanTime = System.currentTimeMillis() - startTime;
            System.out.printf("文件扫描耗时: %d ms%n", scanTime);

            // 步骤2: SpatialQuery 查询
            System.out.println("\n步骤2: SpatialQuery 查询...");
            startTime = System.currentTimeMillis();
            QueryResult queryResults = validator.getQueryResultsInternal(tableName, queryCondition);
            long queryTime = System.currentTimeMillis() - startTime;
            System.out.printf("查询耗时: %d ms%n", queryTime);

            // 步骤3: 比较结果
            System.out.println("\n步骤3: 比较结果...");
            ValidationResult validation = compareResults(baselineResults, queryResults.results);
            validation.printReport();
            if (outputDir != null && !outputDir.trim().isEmpty()) {
                dumpValidationResult(Paths.get(outputDir), i, queryCondition,
                        baselineResults, queryResults, validation);
                System.out.println("结果已写出到目录: " + outputDir);
            }
            hasFailure |= !validation.isValid;
        }

        System.out.println("验证完成！");

        // 如果验证失败，退出码为1
        if (hasFailure) {
            System.exit(1);
        }
    }

    /**
     * 轨迹信息类
     */
    public static class TrajectoryInfo {
        String oid;
        String tid;

        TrajectoryInfo(String oid, String tid) {
            this.oid = oid;
            this.tid = tid;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            TrajectoryInfo that = (TrajectoryInfo) o;
            return Objects.equals(oid, that.oid) && Objects.equals(tid, that.tid);
        }

        @Override
        public int hashCode() {
            return Objects.hash(oid, tid);
        }

        @Override
        public String toString() {
            return oid + "-" + tid;
        }
    }

    /**
     * 验证结果类
     */
    public static class ValidationResult {
        int baselineCount;
        int queryCount;
        Set<TrajectoryInfo> missingTrajectories = new HashSet<>();
        Set<TrajectoryInfo> extraTrajectories = new HashSet<>();
        Set<TrajectoryInfo> correctTrajectories = new HashSet<>();
        boolean isValid;

        public void printReport() {
            System.out.println("\n" + repeatString("=", 80));
            System.out.println("验证结果报告");
            System.out.println(repeatString("=", 80));
            System.out.printf("Baseline 数量 (文件扫描): %d%n", baselineCount);
            System.out.printf("查询结果数量 (SpatialQuery): %d%n", queryCount);
            System.out.printf("正确检测数量: %d%n", correctTrajectories.size());
            System.out.printf("漏检数量: %d%n", missingTrajectories.size());
            System.out.printf("误检数量: %d%n", extraTrajectories.size());
            System.out.printf("验证状态: %s%n", isValid ? "通过" : "失败");

            if (!missingTrajectories.isEmpty()) {
                System.out.println("\n漏检的轨迹 (前20个):");
                int count = 0;
                for (TrajectoryInfo traj : missingTrajectories) {
                    if (count++ < 20) {
                        System.out.println("  - " + traj);
                    } else {
                        System.out.printf("  ... 还有 %d 个%n", missingTrajectories.size() - 20);
                        break;
                    }
                }
            }

            if (!extraTrajectories.isEmpty()) {
                System.out.println("\n误检的轨迹 (前20个):");
                int count = 0;
                for (TrajectoryInfo traj : extraTrajectories) {
                    if (count++ < 20) {
                        System.out.println("  - " + traj);
                    } else {
                        System.out.printf("  ... 还有 %d 个%n", extraTrajectories.size() - 20);
                        break;
                    }
                }
            }
            System.out.println(repeatString("=", 80) + "\n");
        }
    }

    /**
     * 查询结果与统计信息
     */
    public static class QueryResult {
        Set<TrajectoryInfo> results;
        long totalRows;
        long missingIdRows;
        long duplicateRows;

        QueryResult(Set<TrajectoryInfo> results, long totalRows, long missingIdRows, long duplicateRows) {
            this.results = results;
            this.totalRows = totalRows;
            this.missingIdRows = missingIdRows;
            this.duplicateRows = duplicateRows;
        }
    }
}
