package experiments.validate;

import com.esri.core.geometry.Envelope;
import com.esri.core.geometry.GeometryEngine;
import config.TableConfig;
import filter.SpatialFilter;
import index.RangeDebugBridge;
import index.RangeStatsBridge;
import org.locationtech.sfcurve.IndexRange;
import utils.QueryUtils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Compare LETI vs TShape range generation for the same query window.
 * Helps locate why logical range counts are different.
 */
public class RangeFlowCompare {

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.out.println("Usage: RangeFlowCompare <leti_table> <tshape_table> <xmin,ymin,xmax,ymax> [output_dir]");
            System.out.println("Example: RangeFlowCompare tdrive_leti tdrive_tshape \"116.1,39.9,116.2,40.0\" debug/range_diff");
            return;
        }

        String letiTable = args[0].trim();
        String tshapeTable = args[1].trim();
        String query = args[2].trim();
        String outputDir = args.length >= 4 ? args[3].trim() : null;

        Envelope env = parseQueryWindow(query);

        CompareResult leti = runOne(letiTable, RangeStatsBridge.Kind.LETI, env);
        CompareResult tshape = runOne(tshapeTable, RangeStatsBridge.Kind.TSHAPE, env);

        printSummary(query, leti, tshape);
        printSamples(leti, tshape, 20);

        if (outputDir != null && !outputDir.isEmpty()) {
            dumpRanges(outputDir, leti, tshape);
            System.out.println("Detailed range dump written to: " + outputDir);
        }
    }

    private static CompareResult runOne(String tableName, RangeStatsBridge.Kind kind, Envelope env) throws IOException {
        QueryUtils queryUtils = new QueryUtils();
        TableConfig config = queryUtils.getTableConfig(tableName + "_meta");

        String geomWkt = GeometryEngine.geometryToWkt(env, 0);
        SpatialFilter filter = createFilterByKind(kind, geomWkt, config);

        RangeStatsBridge.clearLast(kind);
        if (kind == RangeStatsBridge.Kind.LETI) {
            RangeDebugBridge.clearLastLETI();
        } else if (kind == RangeStatsBridge.Kind.TSHAPE) {
            RangeDebugBridge.clearLastTShape();
        }
        List<IndexRange> ranges = filter.getRanges(tableName, config);
        RangeStatsBridge.Stats stats = RangeStatsBridge.getLast(kind);
        RangeDebugBridge.DebugRanges debugRanges =
                kind == RangeStatsBridge.Kind.LETI ? RangeDebugBridge.getLastLETI() : RangeDebugBridge.getLastTShape();
        return new CompareResult(tableName, kind, ranges, stats, debugRanges);
    }

    private static SpatialFilter createFilterByKind(RangeStatsBridge.Kind kind, String geomWkt, TableConfig config) {
        return new SpatialFilter(geomWkt, config.getCompressType().toString());
    }

    private static void printSummary(String query, CompareResult leti, CompareResult tshape) {
        System.out.println("============================================================");
        System.out.println("RangeFlowCompare");
        System.out.println("============================================================");
        System.out.println("Query window: " + query);
        System.out.println();

        printOneSummary(leti);
        printOneSummary(tshape);

        long logicDelta = leti.ranges.size() - tshape.ranges.size();
        long quadCodeDelta = leti.quadCodeRanges.size() - tshape.quadCodeRanges.size();
        System.out.println();
        System.out.println("LogicRangeCount Delta (LETI - TShape): " + logicDelta);
        System.out.println("QuadCodeRangeCount Delta (LETI - TShape): " + quadCodeDelta);
        boolean sameQuadCode = toRangeKeySet(leti.quadCodeRanges).equals(toRangeKeySet(tshape.quadCodeRanges));
        System.out.println("QuadCode ranges identical? " + sameQuadCode);
        if (sameQuadCode) {
            System.out.println("If LETI qOrder ranges > TShape quadCode ranges, RLOrder locality is weaker here.");
        }
    }

    private static void printOneSummary(CompareResult result) {
        long containedQuad = result.stats == null ? -1L : result.stats.containedQuadCount;
        long intersectQuad = result.stats == null ? -1L : result.stats.intersectQuadCount;
        long visitedCells = result.stats == null ? -1L : result.stats.visitedCellsByContainIntersect();
        long redisAccess = result.stats == null ? -1L : result.stats.redisAccessCount;
        long redisRate = result.stats == null ? -1L : result.stats.redisShapeFilterRate;

        System.out.println("[" + result.kind + "] table=" + result.tableName);
        System.out.println("  logicalRanges       : " + result.ranges.size());
        System.out.println("  quadCodeRanges      : " + result.quadCodeRanges.size());
        if (result.kind == RangeStatsBridge.Kind.LETI) {
            System.out.println("  qOrderRanges        : " + result.qOrderRanges.size());
        }
        System.out.println("  containedQuadCount  : " + containedQuad);
        System.out.println("  intersectQuadCount  : " + intersectQuad);
        System.out.println("  visitedCells(bridge): " + visitedCells);
        System.out.println("  redisAccessCount    : " + redisAccess);
        System.out.println("  redisFilterRate(%)  : " + redisRate);
    }

    private static void printSamples(CompareResult leti, CompareResult tshape, int limit) {
        Set<String> letiQuadSet = toRangeKeySet(leti.quadCodeRanges);
        Set<String> tshapeQuadSet = toRangeKeySet(tshape.quadCodeRanges);
        Set<String> letiSet = toRangeKeySet(leti.ranges);
        Set<String> tshapeSet = toRangeKeySet(tshape.ranges);

        Set<String> onlyLetiQuad = new HashSet<>(letiQuadSet);
        onlyLetiQuad.removeAll(tshapeQuadSet);
        Set<String> onlyTShapeQuad = new HashSet<>(tshapeQuadSet);
        onlyTShapeQuad.removeAll(letiQuadSet);

        Set<String> onlyLeti = new HashSet<>(letiSet);
        onlyLeti.removeAll(tshapeSet);

        Set<String> onlyTshape = new HashSet<>(tshapeSet);
        onlyTshape.removeAll(letiSet);

        System.out.println();
        System.out.println("Only in LETI quadCode ranges: " + onlyLetiQuad.size());
        printSetHead(onlyLetiQuad, "LETI quadCode-only sample", limit);

        System.out.println();
        System.out.println("Only in TShape quadCode ranges: " + onlyTShapeQuad.size());
        printSetHead(onlyTShapeQuad, "TShape quadCode-only sample", limit);

        System.out.println();
        System.out.println("LETI qOrder ranges sample:");
        printSetHead(toRangeKeySet(leti.qOrderRanges), "LETI qOrder sample", limit);

        System.out.println();
        System.out.println("Only in LETI ranges: " + onlyLeti.size());
        printSetHead(onlyLeti, "LETI-only sample", limit);

        System.out.println();
        System.out.println("Only in TShape ranges: " + onlyTshape.size());
        printSetHead(onlyTshape, "TShape-only sample", limit);
    }

    private static Set<String> toRangeKeySet(List<IndexRange> ranges) {
        Set<String> set = new HashSet<>(Math.max(16, ranges.size() * 2));
        for (IndexRange r : ranges) {
            set.add(toRangeKey(r));
        }
        return set;
    }

    private static String toRangeKey(IndexRange r) {
        return r.lower() + ":" + r.upper() + ":" + (r.contained() ? 1 : 0);
    }

    private static void printSetHead(Set<String> set, String title, int limit) {
        System.out.println(title + ":");
        int i = 0;
        for (String v : set) {
            if (i >= limit) {
                break;
            }
            System.out.println("  " + v);
            i++;
        }
        if (set.size() > limit) {
            System.out.println("  ... (" + (set.size() - limit) + " more)");
        }
    }

    private static void dumpRanges(String outputDir, CompareResult leti, CompareResult tshape) throws IOException {
        Path out = Paths.get(outputDir);
        Files.createDirectories(out);

        writeRanges(out.resolve("leti_ranges.csv"), leti.ranges);
        writeRanges(out.resolve("tshape_ranges.csv"), tshape.ranges);
        writeRanges(out.resolve("leti_quadcode_ranges.csv"), leti.quadCodeRanges);
        writeRanges(out.resolve("tshape_quadcode_ranges.csv"), tshape.quadCodeRanges);
        writeRanges(out.resolve("leti_qorder_ranges.csv"), leti.qOrderRanges);

        Set<String> letiSet = toRangeKeySet(leti.ranges);
        Set<String> tshapeSet = toRangeKeySet(tshape.ranges);

        Set<String> onlyLeti = new HashSet<>(letiSet);
        onlyLeti.removeAll(tshapeSet);
        Set<String> onlyTshape = new HashSet<>(tshapeSet);
        onlyTshape.removeAll(letiSet);

        Files.write(out.resolve("only_leti_ranges.txt"), new ArrayList<>(onlyLeti));
        Files.write(out.resolve("only_tshape_ranges.txt"), new ArrayList<>(onlyTshape));
    }

    private static void writeRanges(Path path, List<IndexRange> ranges) throws IOException {
        List<String> lines = new ArrayList<>(ranges.size() + 1);
        lines.add("lower,upper,contained");
        for (IndexRange r : ranges) {
            lines.add(r.lower() + "," + r.upper() + "," + (r.contained() ? 1 : 0));
        }
        Files.write(path, lines);
    }

    private static Envelope parseQueryWindow(String condition) {
        String[] xy = condition.split(",");
        if (xy.length != 4) {
            throw new IllegalArgumentException("Query must be: xmin,ymin,xmax,ymax");
        }
        return new Envelope(
                Double.parseDouble(xy[0].trim()),
                Double.parseDouble(xy[1].trim()),
                Double.parseDouble(xy[2].trim()),
                Double.parseDouble(xy[3].trim())
        );
    }

    private static final class CompareResult {
        final String tableName;
        final RangeStatsBridge.Kind kind;
        final List<IndexRange> ranges;
        final List<IndexRange> quadCodeRanges;
        final List<IndexRange> qOrderRanges;
        final RangeStatsBridge.Stats stats;

        CompareResult(String tableName, RangeStatsBridge.Kind kind, List<IndexRange> ranges, RangeStatsBridge.Stats stats, RangeDebugBridge.DebugRanges debugRanges) {
            this.tableName = tableName;
            this.kind = kind;
            this.ranges = ranges;
            this.stats = stats;
            this.quadCodeRanges = debugRanges == null ? new ArrayList<>() : debugRanges.quadCodeRanges;
            this.qOrderRanges = debugRanges == null ? new ArrayList<>() : debugRanges.qOrderRanges;
        }
    }
}
