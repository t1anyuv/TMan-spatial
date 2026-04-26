package similarity;

import entity.Trajectory;

public final class TrajectorySimilarity {
    public static final int FRECHET = 0;
    public static final int HAUSDORFF = 1;
    public static final int DTW_FUNC = 2;

    private TrajectorySimilarity() {
    }

    public static double calculateDistance(Trajectory query, Trajectory candidate, int func) {
        switch (func) {
            case HAUSDORFF:
                return Hausdorff.calculateDistance(query.getMultiPoint(), candidate.getMultiPoint());
            case DTW_FUNC:
                return DTW.calculateDistance(query.getMultiPoint(), candidate.getMultiPoint());
            case FRECHET:
            default:
                return Frechet.calculateDistance(query.getMultiPoint(), candidate.getMultiPoint());
        }
    }

    public static String functionName(int func) {
        switch (func) {
            case HAUSDORFF:
                return "Hausdorff";
            case DTW_FUNC:
                return "DTW";
            case FRECHET:
            default:
                return "Frechet";
        }
    }
}
