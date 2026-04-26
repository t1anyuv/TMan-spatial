package experiments.common.io;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

import java.io.IOException;

import static client.Constants.META_TABLE;

public final class BenchmarkTableCleaner {

    private static final String LETI_UNION_MASK_SUFFIX = ":leti:union_mask";

    private BenchmarkTableCleaner() {
    }

    public static void deleteTableArtifacts(Admin admin, String tableName, String redisHost) throws IOException {
        deleteHBaseTableIfExists(admin, tableName);
        deleteHBaseTableIfExists(admin, tableName + META_TABLE);
        deleteRedisKeys(tableName, redisHost);
    }

    private static void deleteHBaseTableIfExists(Admin admin, String tableName) throws IOException {
        TableName hTableName = TableName.valueOf(tableName);
        if (!admin.tableExists(hTableName)) {
            return;
        }
        if (!admin.isTableDisabled(hTableName)) {
            admin.disableTable(hTableName);
        }
        admin.deleteTable(hTableName);
    }

    private static void deleteRedisKeys(String tableName, String redisHost) {
        String host = (redisHost == null || redisHost.trim().isEmpty()) ? "127.0.0.1" : redisHost.trim();
        try (Jedis jedis = new Jedis(host, 6379, 60000)) {
            Pipeline pipeline = jedis.pipelined();
            pipeline.del(tableName);
            pipeline.del(tableName + LETI_UNION_MASK_SUFFIX);
            pipeline.sync();
            pipeline.close();
        } catch (Exception e) {
            System.err.println("Error deleting Redis keys for " + tableName + ": " + e.getMessage());
        }
    }
}
