package index;

/**
 * Bridge for passing per-query range stats from Scala index code to Java QueryPlanner
 * without reflection. Stats are stored in a ThreadLocal because queries execute in-thread.
 */
public final class RangeStatsBridge {
    private RangeStatsBridge() {}

    public enum Kind {
        LETI,
        TSHAPE,
        XZ_STAR
    }

    public static final class Stats {
        public final long containedQuadCount;
        public final long intersectQuadCount;
        public final long redisAccessCount;
        /** integer percentage [0,100] */
        public final long redisShapeFilterRate;

        public Stats(long containedQuadCount,
                     long intersectQuadCount,
                     long redisAccessCount,
                     long redisShapeFilterRate) {
            this.containedQuadCount = containedQuadCount;
            this.intersectQuadCount = intersectQuadCount;
            this.redisAccessCount = redisAccessCount;
            this.redisShapeFilterRate = redisShapeFilterRate;
        }

        public long visitedCellsByContainIntersect() {
            return containedQuadCount + intersectQuadCount;
        }
    }

    private static final ThreadLocal<Stats> LETI_LAST = new ThreadLocal<>();
    private static final ThreadLocal<Stats> TSHAPE_LAST = new ThreadLocal<>();
    private static final ThreadLocal<Stats> XZSTAR_LAST = new ThreadLocal<>();

    public static void setLast(Kind kind,
                               long containedQuadCount,
                               long intersectQuadCount,
                               long redisAccessCount,
                               long redisShapeFilterRateScaled) {
        Stats s = new Stats(containedQuadCount, intersectQuadCount, redisAccessCount, redisShapeFilterRateScaled);
        switch (kind) {
            case LETI:
                LETI_LAST.set(s);
                break;
            case TSHAPE:
                TSHAPE_LAST.set(s);
                break;
            case XZ_STAR:
                XZSTAR_LAST.set(s);
                break;
            default:
                break;
        }
    }

    public static Stats getLast(Kind kind) {
        switch (kind) {
            case LETI:
                return LETI_LAST.get();
            case TSHAPE:
                return TSHAPE_LAST.get();
            case XZ_STAR:
                return XZSTAR_LAST.get();
            default:
                return null;
        }
    }

    public static void clearLast(Kind kind) {
        switch (kind) {
            case LETI:
                LETI_LAST.remove();
                break;
            case TSHAPE:
                TSHAPE_LAST.remove();
                break;
            case XZ_STAR:
                XZSTAR_LAST.remove();
                break;
            default:
                break;
        }
    }
}

