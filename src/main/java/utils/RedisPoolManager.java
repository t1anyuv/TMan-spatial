package utils;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Reuse Redis connections across queries to avoid repeated TCP connection setup.
 */
public final class RedisPoolManager {
    private static final int DEFAULT_PORT = 6379;
    private static final int DEFAULT_TIMEOUT_MS = 60_000;

    private static final ConcurrentHashMap<String, JedisPool> POOLS = new ConcurrentHashMap<>();

    private RedisPoolManager() {
    }

    public static Jedis getResource(String host) {
        return getResource(host, DEFAULT_PORT, DEFAULT_TIMEOUT_MS);
    }

    public static Jedis getResource(String host, int port, int timeoutMs) {
        String resolvedHost = (host == null || host.trim().isEmpty()) ? "127.0.0.1" : host.trim();
        String key = resolvedHost + ":" + port + ":" + timeoutMs;
        JedisPool pool = POOLS.computeIfAbsent(key, ignored -> createPool(resolvedHost, port, timeoutMs));
        return pool.getResource();
    }

    private static JedisPool createPool(String host, int port, int timeoutMs) {
        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxTotal(64);
        config.setMaxIdle(16);
        config.setMinIdle(1);
        config.setTestOnBorrow(true);
        config.setTestWhileIdle(true);
        return new JedisPool(config, host, port, timeoutMs);
    }
}
