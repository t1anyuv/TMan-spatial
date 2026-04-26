package utils;

import java.util.Locale;

/**
 * Centralized LETI order path and naming resolver.
 * <p>
 * Responsibilities:
 * - provide a stable default LETI order resource
 * - derive a standard order name / dataset / distribution triple from the resource path
 */
public final class LetiOrderResolver {

    public static final String DEFAULT_DATASET = "tdrive";
    public static final String DEFAULT_DISTRIBUTION = "skew";
    public static final String DEFAULT_CANONICAL_PATH =
            "leti/" + DEFAULT_DATASET + "/" + "default_order.json";

    private LetiOrderResolver() {
    }

    public static String defaultOrderPath() {
        if (!resourceExists(DEFAULT_CANONICAL_PATH)) {
            throw new IllegalStateException("Default LETI order resource not found: " + DEFAULT_CANONICAL_PATH);
        }
        return DEFAULT_CANONICAL_PATH;
    }

    public static String resolveClasspathResource(String resourcePath) {
        String normalized = normalizePath(resourcePath);
        if (normalized.isEmpty()) {
            return defaultOrderPath();
        }
        if (resourceExists(normalized)) {
            return normalized;
        }
        return normalized;
    }

    public static LetiOrderDescriptor resolveDescriptor(String resourcePath) {
        String canonicalPath = resolveClasspathResource(resourcePath);
        String normalized = normalizePath(canonicalPath);

        String dataset = DEFAULT_DATASET;
        String distribution = DEFAULT_DISTRIBUTION;
        String orderName;

        String[] parts = normalized.split("/");
        if (parts.length >= 3 && "leti".equals(parts[0])) {
            dataset = parts[1].toLowerCase(Locale.ROOT);
            String fileName = parts[parts.length - 1].toLowerCase(Locale.ROOT);
            distribution = fileName.endsWith("_order.json")
                    ? fileName.substring(0, fileName.length() - "_order.json".length())
                    : stripJsonSuffix(fileName);
            orderName = "leti." + dataset + "." + distribution;
        } else {
            orderName = "leti.custom." + stripJsonSuffix(parts[parts.length - 1].toLowerCase(Locale.ROOT));
        }

        return new LetiOrderDescriptor(
                resourcePath,
                canonicalPath,
                orderName,
                dataset,
                distribution
        );
    }

    private static boolean resourceExists(String resourcePath) {
        String normalized = normalizePath(resourcePath);
        return LetiOrderResolver.class.getClassLoader().getResource(normalized) != null;
    }

    private static String normalizePath(String resourcePath) {
        if (resourcePath == null) {
            return "";
        }
        return resourcePath.trim().replace('\\', '/').replaceAll("^/+", "");
    }

    private static String stripJsonSuffix(String name) {
        return name.endsWith(".json") ? name.substring(0, name.length() - 5) : name;
    }

    public static final class LetiOrderDescriptor {
        public final String requestedPath;
        public final String canonicalPath;
        public final String orderName;
        public final String dataset;
        public final String distribution;

        public LetiOrderDescriptor(String requestedPath,
                                   String canonicalPath,
                                   String orderName,
                                   String dataset,
                                   String distribution) {
            this.requestedPath = requestedPath;
            this.canonicalPath = canonicalPath;
            this.orderName = orderName;
            this.dataset = dataset;
            this.distribution = distribution;
        }
    }
}
