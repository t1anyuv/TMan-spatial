package experiments.benchmark.io;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Locale;

public final class BenchmarkArtifacts {

    private BenchmarkArtifacts() {
    }

    public static long readTrainingTimeMs(String outputDir, String tableName) {
        java.nio.file.Path path = Paths.get(outputDir, "indexing_time_" + tableName);
        if (!Files.exists(path)) {
            return 0L;
        }
        try {
            String raw = new String(Files.readAllBytes(path), StandardCharsets.UTF_8).trim();
            String digits = raw.replaceAll("[^0-9]", "");
            return digits.isEmpty() ? 0L : Long.parseLong(digits);
        } catch (Exception e) {
            System.err.println("Error reading training time from " + path + ": " + e.getMessage());
            return 0L;
        }
    }

    public static long estimateIndexSizeKb(String tableName, String redisHost) {
        long hbaseBytes = estimateHBaseTableBytes(tableName);
        long redisBytes = estimateRedisBytes(tableName, redisHost);
        return bytesToKb(hbaseBytes + redisBytes);
    }

    private static long estimateHBaseTableBytes(String tableName) {
        Configuration conf = HBaseConfiguration.create();
        try {
            Path rootDir = CommonFSUtils.getRootDir(conf);
            Path tableDir = CommonFSUtils.getTableDir(rootDir, TableName.valueOf(tableName));
            FileSystem fs = tableDir.getFileSystem(conf);
            if (!fs.exists(tableDir)) {
                return 0L;
            }
            return sumPath(fs, tableDir);
        } catch (Exception e) {
            System.err.println("Error estimating HBase table size for " + tableName + ": " + e.getMessage());
            return 0L;
        }
    }

    private static long sumPath(FileSystem fs, Path path) throws IOException {
        FileStatus status = fs.getFileStatus(path);
        if (status.isFile()) {
            return status.getLen();
        }
        long total = 0L;
        for (FileStatus child : fs.listStatus(path)) {
            total += sumPath(fs, child.getPath());
        }
        return total;
    }

    private static long estimateRedisBytes(String tableName, String redisHost) {
        String host = (redisHost == null || redisHost.trim().isEmpty()) ? "127.0.0.1" : redisHost.trim();
        try (Jedis jedis = new Jedis(host, 6379, 60000)) {
            Long size = jedis.memoryUsage(tableName);
            return size == null ? 0L : size;
        } catch (Exception e) {
            System.err.printf(Locale.ROOT,
                    "Error estimating Redis size for key %s on %s: %s%n",
                    tableName, host, e.getMessage());
            return 0L;
        }
    }

    private static long bytesToKb(long bytes) {
        if (bytes <= 0L) {
            return 0L;
        }
        return (bytes + 1023L) / 1024L;
    }
}
