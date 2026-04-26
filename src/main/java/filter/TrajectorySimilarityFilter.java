package filter;

import entity.Trajectory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.util.Bytes;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.MultiPoint;
import org.locationtech.jts.io.WKTReader;
import preprocess.compress.IIntegerCompress;
import similarity.TrajectorySimilarity;
import utils.TrajectoryParser;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static client.Constants.END_POINT;
import static client.Constants.GEOM_X;
import static client.Constants.GEOM_Y;
import static client.Constants.GEOM_Z;
import static client.Constants.PIVOT_MBR;
import static client.Constants.PIVOT_POINT;
import static client.Constants.START_POINT;

public class TrajectorySimilarityFilter extends FilterBase {
    private static final GeometryFactory GEOMETRY_FACTORY = new GeometryFactory();
    private static final WKTReader WKT_READER = new WKTReader(GEOMETRY_FACTORY);
    private static final double COORDINATE_SCALE = 1_000_000.0d;

    private final String queryTrajectoryWkt;
    private final String compressType;
    private final double threshold;
    private final int func;
    private final boolean applyDistanceThreshold;

    private transient Trajectory queryTrajectory;
    private transient Geometry queryStart;
    private transient Geometry queryEnd;
    private transient Geometry queryMbr;
    private transient List<Integer> queryPivotIndexes;

    private boolean filterRow;
    private boolean checked;
    private boolean foundX;
    private boolean foundY;
    private boolean foundZ;
    private boolean foundPivotMbr;
    private byte[] xBytes;
    private byte[] yBytes;
    private byte[] zBytes;
    private Trajectory candidateTrajectory;
    private Geometry candidateMbr;
    private String candidatePivotIndexes;

    public TrajectorySimilarityFilter(String queryTrajectoryWkt,
                                      String compressType,
                                      double threshold,
                                      int func,
                                      boolean applyDistanceThreshold) {
        this.queryTrajectoryWkt = queryTrajectoryWkt;
        this.compressType = compressType;
        this.threshold = threshold;
        this.func = func;
        this.applyDistanceThreshold = applyDistanceThreshold;
        initQueryState();
    }

    @Override
    public ReturnCode filterCell(Cell cell) throws IOException {
        if (filterRow || checked) {
            return filterRow ? ReturnCode.NEXT_ROW : ReturnCode.INCLUDE;
        }

        String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
        byte[] value = CellUtil.cloneValue(cell);

        if (START_POINT.equals(qualifier) && func == TrajectorySimilarity.FRECHET) {
            Geometry start = parseWkt(Bytes.toString(value));
            if (start != null && start.distance(queryStart) > threshold) {
                filterRow = true;
                return ReturnCode.NEXT_ROW;
            }
        } else if (END_POINT.equals(qualifier) && func == TrajectorySimilarity.FRECHET) {
            Geometry end = parseWkt(Bytes.toString(value));
            if (end != null && end.distance(queryEnd) > threshold) {
                filterRow = true;
                return ReturnCode.NEXT_ROW;
            }
        } else if (PIVOT_MBR.equals(qualifier)) {
            foundPivotMbr = true;
            candidateMbr = parseWkt(Bytes.toString(value));
            if (candidateMbr != null && !passesMbrFilter(candidateMbr)) {
                filterRow = true;
                return ReturnCode.NEXT_ROW;
            }
        } else if (PIVOT_POINT.equals(qualifier)) {
            candidatePivotIndexes = Bytes.toString(value);
        } else if (GEOM_X.equals(qualifier)) {
            foundX = true;
            xBytes = value;
        } else if (GEOM_Y.equals(qualifier)) {
            foundY = true;
            yBytes = value;
        } else if (GEOM_Z.equals(qualifier)) {
            foundZ = true;
            zBytes = value;
        }

        if (foundX && foundY && foundZ) {
            candidateTrajectory = decodeTrajectory(xBytes, yBytes, zBytes);
            if (candidateTrajectory == null) {
                filterRow = true;
                return ReturnCode.NEXT_ROW;
            }
        }

        return ReturnCode.INCLUDE;
    }

    @Override
    public boolean filterRow() {
        if (!checked && !filterRow) {
            if (foundX && foundY && foundZ && candidateTrajectory == null) {
                filterRow = true;
            } else if (!hasRequiredCells()) {
                filterRow = false;
            } else {
                if (!passesPivotFilter(candidateTrajectory)) {
                    filterRow = true;
                } else if (applyDistanceThreshold) {
                    double distance = TrajectorySimilarity.calculateDistance(queryTrajectory, candidateTrajectory, func);
                    filterRow = distance > threshold;
                }
            }
            checked = true;
        }
        return filterRow;
    }

    @Override
    public void reset() {
        filterRow = false;
        checked = false;
        foundX = false;
        foundY = false;
        foundZ = false;
        foundPivotMbr = false;
        xBytes = null;
        yBytes = null;
        zBytes = null;
        candidateTrajectory = null;
        candidateMbr = null;
        candidatePivotIndexes = null;
    }

    @Override
    public byte[] toByteArray() {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(baos);
            out.writeUTF(queryTrajectoryWkt);
            out.writeUTF(compressType);
            out.writeDouble(threshold);
            out.writeInt(func);
            out.writeBoolean(applyDistanceThreshold);
            out.flush();
            return baos.toByteArray();
        } catch (IOException e) {
            throw new IllegalStateException("Failed to serialize TrajectorySimilarityFilter", e);
        }
    }

    public static Filter parseFrom(final byte[] bytes) {
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes))) {
            return new TrajectorySimilarityFilter(
                    in.readUTF(),
                    in.readUTF(),
                    in.readDouble(),
                    in.readInt(),
                    in.readBoolean()
            );
        } catch (IOException e) {
            throw new IllegalArgumentException("Failed to deserialize TrajectorySimilarityFilter", e);
        }
    }

    private void initQueryState() {
        TrajectoryParser.ParsedTrajectory parsed = TrajectoryParser.parse(queryTrajectoryWkt);
        this.queryTrajectory = new Trajectory(parsed.oid, parsed.tid, parsed.jtsGeo);
        this.queryStart = queryTrajectory.getGeometryN(0);
        this.queryEnd = queryTrajectory.getGeometryN(queryTrajectory.getNumGeometries() - 1);
        this.queryMbr = queryTrajectory.getDPFeature().getMBRs();
        this.queryPivotIndexes = queryTrajectory.getDPFeature().getIndexes();
    }

    private boolean passesMbrFilter(Geometry candidate) {
        for (int i = 0; i < candidate.getNumGeometries(); i++) {
            if (candidate.getGeometryN(i).distance(queryMbr) > threshold) {
                return false;
            }
        }
        for (int i = 0; i < queryMbr.getNumGeometries(); i++) {
            if (queryMbr.getGeometryN(i).distance(candidate) > threshold) {
                return false;
            }
        }
        return true;
    }

    private boolean passesPivotFilter(Trajectory candidate) {
        if (candidateMbr == null) {
            return true;
        }

        for (Integer queryPivotIndex : queryPivotIndexes) {
            if (candidateMbr != null && queryTrajectory.getGeometryN(queryPivotIndex).distance(candidateMbr) > threshold) {
                return false;
            }
        }

        for (Integer candidatePivotIndex : parsePivotIndexes(candidatePivotIndexes)) {
            if (candidatePivotIndex >= 0 && candidatePivotIndex < candidate.getNumGeometries()) {
                if (candidate.getGeometryN(candidatePivotIndex).distance(queryMbr) > threshold) {
                    return false;
                }
            }
        }
        return true;
    }

    private boolean hasRequiredCells() {
        return foundX && foundY && foundZ && foundPivotMbr;
    }

    private Trajectory decodeTrajectory(byte[] encodedX, byte[] encodedY, byte[] encodedZ) {
        IIntegerCompress compressor = IIntegerCompress.getIntegerCompress(compressType);
        int[] xs = compressor.decoding(encodedX);
        int[] ys = compressor.decoding(encodedY);
        int[] zs = compressor.decoding(encodedZ);
        int size = Math.min(xs.length, Math.min(ys.length, zs.length));
        if (size <= 0) {
            return null;
        }

        Coordinate[] coordinates = new Coordinate[size];
        for (int i = 0; i < size; i++) {
            Coordinate coordinate = new Coordinate(xs[i] / COORDINATE_SCALE, ys[i] / COORDINATE_SCALE);
            coordinate.setZ(zs[i]);
            coordinates[i] = coordinate;
        }
        MultiPoint multiPoint = GEOMETRY_FACTORY.createMultiPointFromCoords(coordinates);
        return new Trajectory("", "", multiPoint);
    }

    private static List<Integer> parsePivotIndexes(String indexesText) {
        if (indexesText == null || indexesText.isEmpty()) {
            return java.util.Collections.emptyList();
        }
        String[] parts = indexesText.split(",");
        List<Integer> indexes = new ArrayList<>(parts.length);
        for (String part : parts) {
            if (part == null || part.isEmpty()) {
                continue;
            }
            indexes.add(Integer.parseInt(part));
        }
        return indexes;
    }

    private static Geometry parseWkt(String wkt) {
        try {
            return WKT_READER.read(wkt);
        } catch (Exception e) {
            return null;
        }
    }
}
