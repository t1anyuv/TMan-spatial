package similarity;

import org.locationtech.jts.geom.Geometry;
import utils.NumberUtil;

public final class Frechet {
    private Frechet() {
    }

    public static double calculateDistance(Geometry search, Geometry queried) {
        int n = search.getNumGeometries();
        int m = queried.getNumGeometries();
        double[][] minDis = new double[n][m];
        minDis[0][0] = search.getGeometryN(0).distance(queried.getGeometryN(0));

        for (int i = 1; i < n; i++) {
            minDis[i][0] = Math.max(minDis[i - 1][0], search.getGeometryN(i).distance(queried.getGeometryN(0)));
        }

        for (int j = 1; j < m; j++) {
            minDis[0][j] = Math.max(minDis[0][j - 1], search.getGeometryN(0).distance(queried.getGeometryN(j)));
        }

        for (int i = 1; i < n; i++) {
            for (int j = 1; j < m; j++) {
                minDis[i][j] = Math.max(
                        NumberUtil.min(minDis[i - 1][j], minDis[i][j - 1], minDis[i - 1][j - 1]),
                        search.getGeometryN(i).distance(queried.getGeometryN(j))
                );
            }
        }
        return minDis[n - 1][m - 1];
    }
}
