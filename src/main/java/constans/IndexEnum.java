package constans;


import lombok.Getter;

public class IndexEnum {
    @Getter
    public enum INDEX_TYPE {
        SPATIAL("spatial"),
        TEMPORAL("temporal"),
        ST("st"),
        OID("oid");
        private final String indexName;

        INDEX_TYPE(String indexName) {
            this.indexName = indexName;
        }

    }
}
