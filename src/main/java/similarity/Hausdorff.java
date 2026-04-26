package similarity;

import org.locationtech.jts.geom.Geometry;

public final class Hausdorff {
    private Hausdorff() {
    }

    public static double calculateDistance(Geometry search, Geometry queried) {
        int n = search.getNumGeometries();
        int m = queried.getNumGeometries();
        double maxDis = 0.0;

        for (int i = 0; i < n; i++) {
            double minD = Double.MAX_VALUE;
            for (int j = 0; j < m; j++) {
                double dis = search.getGeometryN(i).distance(queried.getGeometryN(j));
                if (dis < minD) {
                    minD = dis;
                }
            }
            if (maxDis < minD) {
                maxDis = minD;
            }
        }

        for (int i = 0; i < m; i++) {
            double minD = Double.MAX_VALUE;
            for (int j = 0; j < n; j++) {
                double dis = search.getGeometryN(j).distance(queried.getGeometryN(i));
                if (dis < minD) {
                    minD = dis;
                }
            }
            if (maxDis < minD) {
                maxDis = minD;
            }
        }
        return maxDis;
    }
}
