package constans;


public class IndexEnum {
    public enum INDEX_TYPE {
        SPATIAL("spatial"),
        TEMPORAL("temporal"),
        ST("st"),
        OID("oid");
        private final String indexName;

        INDEX_TYPE(String indexName) {
            this.indexName = indexName;
        }

        public String getIndexName() {
            return indexName;
        }
    }
}
