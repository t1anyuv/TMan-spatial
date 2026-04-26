package utils;

import com.esri.core.geometry.Geometry;
import com.esri.core.geometry.Operator;
import com.esri.core.geometry.OperatorFactoryLocal;
import com.esri.core.geometry.OperatorImportFromWkt;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.MultiPoint;

import java.io.Serializable;

/**
 * 轨迹数据解析器
 * 支持多种轨迹数据格式:
 * 1. 标准格式: oid-tid-wkt
 *    例如: 1-3644-MULTIPOINT((116.37497 39.85789), (116.37542 39.85764))
 * 2. 管道格式: oid|tid|sid|points
 *    例如: 1|3644|1|116.37497,39.85789;116.37542,39.85764;...
 * 3. CSV WKT格式: oid,tid,sid,LINESTRING/WKT
 *    例如: 1,3644,1,LINESTRING(116.37497 39.85789, 116.37542 39.85764)
 */
public class TrajectoryParser implements Serializable {

    private static final GeometryFactory JTS_FACTORY = new GeometryFactory();

    /**
     * 解析后的轨迹数据
     */
    public static class ParsedTrajectory implements Serializable {
        public final String oid;
        public final String tid;
        public final String sid;
        public final com.esri.core.geometry.MultiPoint esriGeo;
        public final org.locationtech.jts.geom.Geometry jtsGeo;
        public final String wkt;

        public ParsedTrajectory(String oid, String tid, String sid,
                                com.esri.core.geometry.MultiPoint esriGeo,
                                org.locationtech.jts.geom.Geometry jtsGeo,
                                String wkt) {
            this.oid = oid;
            this.tid = tid;
            this.sid = sid;
            this.esriGeo = esriGeo;
            this.jtsGeo = jtsGeo;
            this.wkt = wkt;
        }
    }

    /**
     * 自动检测并解析轨迹数据格式
     *
     * @param traj 轨迹字符串
     * @return 解析后的轨迹数据
     */
    public static ParsedTrajectory parse(String traj) {
        if (traj == null || traj.isEmpty()) {
            throw new IllegalArgumentException("Trajectory string is null or empty");
        }

        if (traj.contains("|")) {
            return parsePipeFormat(traj);
        }

        int wktStart = findWktStart(traj);
        if (wktStart != -1) {
            String header = traj.substring(0, wktStart).trim();
            if (looksLikeCsvWktHeader(header)) {
                return parseCsvWktFormat(traj);
            }
            if (looksLikeStandardHeader(header)) {
                return parseStandardFormat(traj);
            }
        }

        if (traj.contains("-")) {
            return parseStandardFormat(traj);
        }

        throw new IllegalArgumentException(
                "Unknown trajectory format: " + traj.substring(0, Math.min(50, traj.length()))
        );
    }

    /**
     * 解析标准格式: oid-tid-wkt
     * 例如: 1-3644-MULTIPOINT((116.37497 39.85789), (116.37542 39.85764))
     * 或: 1-3644-LINESTRING(116.37497 39.85789, 116.37542 39.85764)
     */
    private static ParsedTrajectory parseStandardFormat(String traj) {
        String[] parts = traj.split("-", 3);
        if (parts.length < 3) {
            throw new IllegalArgumentException("Invalid standard format (expected oid-tid-wkt): " + traj);
        }
        String oid = parts[0];
        String tid = parts[1];
        String wkt = parts[2];
        String sid = "";

        com.esri.core.geometry.MultiPoint esriGeo = parseWKTToMultiPoint(wkt);
        org.locationtech.jts.geom.Geometry jtsGeo = parseWKTToJTS(wkt);

        return new ParsedTrajectory(oid, tid, sid, esriGeo, jtsGeo, wkt);
    }

    /**
     * 解析管道格式: oid|tid|sid|points
     * 例如: 1|3644|1|116.37497,39.85789;116.37542,39.85764;...
     */
    private static ParsedTrajectory parsePipeFormat(String traj) {
        String[] parts = traj.split("\\|", 4);
        if (parts.length < 4) {
            throw new IllegalArgumentException("Invalid pipe format (expected oid|tid|sid|points): " + traj);
        }
        String oid = parts[0];
        String tid = parts[1];
        String sid = parts[2];
        String pointsStr = parts[3];

        String[] pointPairs = pointsStr.split(";");
        java.util.List<Coordinate> coords = new java.util.ArrayList<>();

        for (String pointPair : pointPairs) {
            if (pointPair.trim().isEmpty()) {
                continue;
            }
            String[] coord = pointPair.split(",");
            if (coord.length >= 2) {
                double x = Double.parseDouble(coord[0].trim());
                double y = Double.parseDouble(coord[1].trim());
                coords.add(new Coordinate(x, y));
            }
        }

        if (coords.isEmpty()) {
            throw new IllegalArgumentException("No valid coordinates found in: " + pointsStr);
        }

        Coordinate[] coordArray = coords.toArray(new Coordinate[0]);
        MultiPoint jtsMultiPoint = JTS_FACTORY.createMultiPointFromCoords(coordArray);
        String wkt = coordsToWKT(coords);
        com.esri.core.geometry.MultiPoint esriGeo = coordsToESRIMultiPoint(coords);

        return new ParsedTrajectory(oid, tid, sid, esriGeo, jtsMultiPoint, wkt);
    }

    /**
     * 解析CSV WKT格式: oid,tid,sid,LINESTRING/WKT
     * 例如: 1,3644,1,LINESTRING(116.37497 39.85789, 116.37542 39.85764)
     * 或: 1,3644,1,MULTIPOINT((116.37497 39.85789), (116.37542 39.85764))
     */
    private static ParsedTrajectory parseCsvWktFormat(String traj) {
        int wktStart = findWktStart(traj);
        if (wktStart == -1) {
            throw new IllegalArgumentException("No WKT geometry found in: " + traj);
        }

        String header = traj.substring(0, wktStart);
        String wkt = traj.substring(wktStart);

        String[] headerParts = header.split(",");
        if (headerParts.length < 3) {
            throw new IllegalArgumentException("Invalid CSV WKT format (expected oid,tid,sid,WKT): " + traj);
        }
        String oid = headerParts[0];
        String tid = headerParts[1];
        String sid = headerParts[2];

        String multiPointWkt = convertToMultiPointWKT(wkt);

        com.esri.core.geometry.MultiPoint esriGeo = parseWKTToMultiPoint(multiPointWkt);
        org.locationtech.jts.geom.Geometry jtsGeo = parseWKTToJTS(multiPointWkt);

        return new ParsedTrajectory(oid, tid, sid, esriGeo, jtsGeo, multiPointWkt);
    }

    private static int findWktStart(String traj) {
        int wktStart = traj.indexOf("LINESTRING");
        if (wktStart == -1) {
            wktStart = traj.indexOf("MULTIPOINT");
        }
        if (wktStart == -1) {
            wktStart = traj.indexOf("POINT");
        }
        return wktStart;
    }

    private static boolean looksLikeCsvWktHeader(String header) {
        String[] parts = header.split(",", -1);
        return parts.length >= 3;
    }

    private static boolean looksLikeStandardHeader(String header) {
        int firstDash = header.indexOf('-');
        if (firstDash <= 0) {
            return false;
        }
        int secondDash = header.indexOf('-', firstDash + 1);
        return secondDash > firstDash + 1;
    }

    /**
     * 将坐标列表转换为 WKT MULTIPOINT 字符串
     */
    private static String coordsToWKT(java.util.List<Coordinate> coords) {
        StringBuilder sb = new StringBuilder("MULTIPOINT(");
        for (int i = 0; i < coords.size(); i++) {
            Coordinate c = coords.get(i);
            sb.append("(").append(c.getX()).append(" ").append(c.getY()).append(")");
            if (i < coords.size() - 1) {
                sb.append(", ");
            }
        }
        sb.append(")");
        return sb.toString();
    }

    /**
     * 将坐标列表转换为 ESRI MultiPoint
     */
    private static com.esri.core.geometry.MultiPoint coordsToESRIMultiPoint(java.util.List<Coordinate> coords) {
        com.esri.core.geometry.MultiPoint mp = new com.esri.core.geometry.MultiPoint();
        for (Coordinate c : coords) {
            mp.add(c.getX(), c.getY());
        }
        return mp;
    }

    /**
     * 将 LINESTRING WKT 转换为 MULTIPOINT WKT
     */
    private static String convertToMultiPointWKT(String wkt) {
        if (wkt.startsWith("MULTIPOINT")) {
            return wkt;
        }
        if (wkt.startsWith("LINESTRING")) {
            String coords = wkt.substring(wkt.indexOf('(') + 1, wkt.lastIndexOf(')'));
            String[] points = coords.split(",");
            StringBuilder sb = new StringBuilder("MULTIPOINT(");
            for (int i = 0; i < points.length; i++) {
                sb.append("(").append(points[i].trim()).append(")");
                if (i < points.length - 1) {
                    sb.append(", ");
                }
            }
            sb.append(")");
            return sb.toString();
        }
        return wkt;
    }

    /**
     * 解析 WKT 为 ESRI MultiPoint
     */
    private static com.esri.core.geometry.MultiPoint parseWKTToMultiPoint(String wkt) {
        OperatorImportFromWkt importerWKT = (OperatorImportFromWkt) OperatorFactoryLocal.getInstance()
                .getOperator(Operator.Type.ImportFromWkt);
        return (com.esri.core.geometry.MultiPoint) importerWKT.execute(0, Geometry.Type.MultiPoint, wkt, null);
    }

    /**
     * 解析 WKT 为 JTS Geometry
     */
    private static org.locationtech.jts.geom.Geometry parseWKTToJTS(String wkt) {
        try {
            org.locationtech.jts.io.WKTReader reader = new org.locationtech.jts.io.WKTReader();
            return reader.read(wkt);
        } catch (org.locationtech.jts.io.ParseException e) {
            throw new RuntimeException("Failed to parse WKT: " + wkt, e);
        }
    }
}
