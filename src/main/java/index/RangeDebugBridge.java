package index;

import org.locationtech.sfcurve.IndexRange;

import java.util.Collections;
import java.util.List;

/**
 * Bridge for exposing per-query debug ranges from Scala index implementations.
 */
public final class RangeDebugBridge {
    private RangeDebugBridge() {
    }

    public static final class DebugRanges {
        public final List<IndexRange> quadCodeRanges;
        public final List<IndexRange> qOrderRanges;

        public DebugRanges(List<IndexRange> quadCodeRanges, List<IndexRange> qOrderRanges) {
            this.quadCodeRanges = quadCodeRanges == null ? Collections.emptyList() : quadCodeRanges;
            this.qOrderRanges = qOrderRanges == null ? Collections.emptyList() : qOrderRanges;
        }
    }

    private static final ThreadLocal<DebugRanges> LETI_LAST = new ThreadLocal<>();
    private static final ThreadLocal<DebugRanges> TSHAPE_LAST = new ThreadLocal<>();

    public static void setLastLETI(List<IndexRange> quadCodeRanges, List<IndexRange> qOrderRanges) {
        LETI_LAST.set(new DebugRanges(quadCodeRanges, qOrderRanges));
    }

    public static void setLastTShape(List<IndexRange> quadCodeRanges) {
        TSHAPE_LAST.set(new DebugRanges(quadCodeRanges, Collections.emptyList()));
    }

    public static DebugRanges getLastLETI() {
        return LETI_LAST.get();
    }

    public static DebugRanges getLastTShape() {
        return TSHAPE_LAST.get();
    }

    public static void clearLastLETI() {
        LETI_LAST.remove();
    }

    public static void clearLastTShape() {
        TSHAPE_LAST.remove();
    }
}
