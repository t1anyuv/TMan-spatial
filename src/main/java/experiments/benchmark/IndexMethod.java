package experiments.benchmark;

import lombok.Getter;

/**
 * 支持的索引方法枚举
 */
@Getter
public enum IndexMethod {
    LETI("leti", "LETILocSIndex"),
    TSHAPE("tshape", "LocSIndex"),
    XZ_STAR("xzstar", "XZStarIndex"),
    LMSFC("LMSFC", "LMSFCIndex"),
    BMTREE("BMT", "BMTreeIndex");

    private final String shortName;
    private final String indexClassName;

    IndexMethod(String shortName, String indexClassName) {
        this.shortName = shortName;
        this.indexClassName = indexClassName;
    }

}
