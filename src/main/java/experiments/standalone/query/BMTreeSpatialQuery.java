package experiments.standalone.query;

import com.esri.core.geometry.Envelope;
import com.esri.core.geometry.Geometry;
import com.esri.core.geometry.GeometryEngine;
import com.esri.core.geometry.Operator;
import com.esri.core.geometry.OperatorFactoryLocal;
import com.esri.core.geometry.OperatorImportFromWkt;
import config.TableConfig;
import filter.SpatialWithSFC;
import org.apache.hadoop.hbase.filter.FilterBase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * BMTree索引空间查询实验程序
 * <p>
 * 使用BMTree索引执行空间范围查询
 * <p>
 * 命令行参数：
 * [0] tableName - HBase表名
 * [1] queryConditions - 查询条件（分号分隔），每个条件格式为 "xmin,ymin,xmax,ymax"
 * [2] resultPath - 结果输出路径（CSV文件）
 * <p>
 * 查询条件格式：
 * - 单个查询: "116.1,39.6,116.5,40.0"
 * - 多个查询: "116.1,39.6,116.5,40.0;116.2,39.7,116.6,40.1;..."
 * <p>
 * 输出结果：
 * - CSV文件包含查询性能统计信息：
 * - time: 查询执行时间（毫秒）
 * - indexRanges: 索引范围数量
 * - candidates: 候选结果数量（过滤前）
 * - finalSize: 最终结果数量（过滤后）
 * - 每个指标包含：min, max, avg, median, per70, per80, per90
 * <p>
 * 示例：
 * java experiments.standalone.query.BMTreeSpatialQuery \
 * bmtree_table \
 * "116.1,39.6,116.5,40.0;116.2,39.7,116.6,40.1" \
 * /results/bmtree_query_results.csv
 */
public class BMTreeSpatialQuery extends SpatialQuery {
    public static void main(String[] args) throws IOException {
        if (args.length < 3) {
            printUsage();
            System.exit(1);
        }

        System.out.println("\n" + "================================================================================");
        System.out.println("BMTree索引空间查询");
        System.out.println("================================================================================");
        System.out.println("索引类型: BMTree (Binary Multi-dimensional Tree)");
        System.out.println("查询类型: 空间范围查询");
        System.out.println("过滤器  : SpatialWithSFC");
        System.out.println("================================================================================\n");

        BMTreeSpatialQuery stQuery = new BMTreeSpatialQuery();
        stQuery.executeQuery(args);
    }

    /**
     * 重写 getFilters 方法，使用 SpatialWithSFC Filter
     * 
     * @param condition   查询条件字符串（格式：xmin,ymin,xmax,ymax）
     * @param tableConfig 表配置
     * @return Filter 列表
     */
    @Override
    public List<FilterBase> getFilters(String condition, TableConfig tableConfig) {
        // 1. 解析查询条件
        String[] xy = condition.trim().split(",");
        Envelope env = new Envelope(
                Double.parseDouble(xy[0].trim()),
                Double.parseDouble(xy[1].trim()),
                Double.parseDouble(xy[2].trim()),
                Double.parseDouble(xy[3].trim())
        );

        // 2. 验证配置
        if (tableConfig.getBMTreeConfigPath() == null || tableConfig.getBMTreeConfigPath().isEmpty()) {
            throw new IllegalArgumentException("BMTreeConfigPath 不能为空，BMTree 索引需要配置文件路径");
        }

        if (!tableConfig.isBMTree()) {
            System.err.println("[警告] 表配置未标记为 BMTree 索引，可能导致查询错误");
        }

        // 3. 创建 BMTreeIndex 实例
        index.BMTreeIndex bmtreeIndex;
        if (tableConfig.getEnvelope() != null) {
            bmtreeIndex = index.BMTreeIndex.apply(
                    (short) tableConfig.getResolution(),
                    new scala.Tuple2<>(
                            tableConfig.getEnvelope().getXMin(),
                            tableConfig.getEnvelope().getXMax()),
                    new scala.Tuple2<>(
                            tableConfig.getEnvelope().getYMin(),
                            tableConfig.getEnvelope().getYMax()),
                    tableConfig.getBMTreeConfigPath(),
                    tableConfig.getBMTreeBitLength());
        } else {
            bmtreeIndex = index.BMTreeIndex.apply(
                    (short) tableConfig.getResolution(),
                    new scala.Tuple2<>(-180.0, 180.0),
                    new scala.Tuple2<>(-90.0, 90.0),
                    tableConfig.getBMTreeConfigPath(),
                    tableConfig.getBMTreeBitLength());
        }

        // 4. 将查询窗口转换为几何对象
        String wkt = GeometryEngine.geometryToWkt(env, 0);
        OperatorImportFromWkt importerWKT = (OperatorImportFromWkt) OperatorFactoryLocal.getInstance()
                .getOperator(Operator.Type.ImportFromWkt);
        Geometry queryGeometry = importerWKT.execute(
                0,
                Geometry.Type.Polygon,
                wkt,
                null);

        // 5. 计算查询窗口的索引范围
        java.util.List<org.locationtech.sfcurve.IndexRange> ranges = bmtreeIndex.ranges(
                env.getXMin(), env.getYMin(), env.getXMax(), env.getYMax());

        // 计算所有范围中的最小 SFC 值
        scala.Tuple3<Object, Object, Object> queryIndex = bmtreeIndex.index(queryGeometry, false);
        long queryMinSFC = (long) queryIndex._2();
        if (ranges.isEmpty()) {
            queryMinSFC = 0L;
        }

        // 打印调试信息
        System.out.printf(
                "[BMTreeSpatialQuery] 查询窗口: [%.4f, %.4f] - [%.4f, %.4f]%n",
                env.getXMin(), env.getYMin(), env.getXMax(), env.getYMax());

        // 6. 创建 SpatialWithSFC Filter
        SpatialWithSFC spatialFilter = new SpatialWithSFC(
                wkt,
                tableConfig.getCompressType().name(),
                queryMinSFC);

        // 7. 返回 Filter 列表
        List<FilterBase> filters = new ArrayList<>();
        filters.add(spatialFilter);
        return filters;
    }

    private static void printUsage() {
        System.err.println("用法: BMTreeSpatialQuery <tableName> <queryConditions> <resultPath>");
        System.err.println("\n参数:");
        System.err.println("  tableName        - HBase表名");
        System.err.println("  queryConditions  - 查询条件（分号分隔）");
        System.err.println("                     格式: \"xmin,ymin,xmax,ymax\"");
        System.err.println("  resultPath       - 结果输出路径（CSV文件）");
        System.err.println("\n示例:");
        System.err.println("  BMTreeSpatialQuery bmtree_table \\");
        System.err.println("    \"116.1,39.6,116.5,40.0;116.2,39.7,116.6,40.1\" \\");
        System.err.println("    /results/bmtree_query.csv");
    }
}
