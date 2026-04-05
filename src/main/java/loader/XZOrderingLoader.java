package loader;

import config.TableConfig;
import utils.ByteArraysWrapper;
import utils.TrajPutUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapred.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapred.JobConf;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import scala.Tuple2;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static constans.IndexEnum.INDEX_TYPE.*;

/**
 * XZ Ordering 索引加载器
 * 继承自 Loader，用于加载使用 XZ Ordering 索引策略的数据到 HBase 和 Redis
 * XZ Ordering 是一种基于 XZ 曲线排序的空间索引方法
 *
 */
public class XZOrderingLoader extends Loader {
    private final TableConfig config;

    /**
     * 构造函数
     *
     * @param config 表配置
     * @param tableName 表名
     * @param sourcePath 源数据路径
     * @param resultPath 结果路径
     * @throws IOException IO 异常
     */
    public XZOrderingLoader(TableConfig config, String tableName, String sourcePath, String resultPath) throws IOException {
        super(config, tableName, sourcePath, resultPath);
        this.config = config;
    }

    /**
     * 构造函数（使用配置中的表名）
     *
     * @param config 表配置
     * @param sourcePath 源数据路径
     * @param resultPath 结果路径
     * @throws IOException IO 异常
     */
    public XZOrderingLoader(TableConfig config, String sourcePath, String resultPath) throws IOException {
        this(config, config.getTableName(), sourcePath, resultPath);
    }

    /**
     * 获取轨迹的二级索引映射（XZ Ordering 版本）
     * <p>
     * XZ Ordering 使用空间作为主索引，不需要二级索引。
     *
     * @param traj 轨迹字符串，格式为 "oid-tid-wkt"
     * @param rowKey 主表的行键
     * @return 空的二级索引映射
     */
    @Override
    Map<String, Tuple2<Long, byte[]>> getSecondaryIndex(String traj, byte[] rowKey) {
        // 断言：XZ Ordering 必须使用空间主索引
        assert config.getPrimary().equals(SPATIAL) : "XZ Ordering only supports SPATIAL primary index";

        // 空间是主索引，不需要其他二级索引
        return new HashMap<>();
    }

    /**
     * 存储 XZ Ordering 二级索引表到 Redis
     *
     * @param indexedRDD 包含 Put 操作和二级索引映射的 RDD
     */
    private void storeXZSecondaryTable(JavaRDD<Tuple2<Put, Map<String, Tuple2<Long, byte[]>>>> indexedRDD) {
        indexedRDD.map(v -> {
            v._2.remove(tableName);
            return v;
        }).foreachPartition(v -> {
            int size = 0;
            Jedis jedis = new Jedis(config.getRedisHost(), 6379);
            Pipeline pipelined = jedis.pipelined();
            while (v.hasNext()) {
                for (Map.Entry<String, Tuple2<Long, byte[]>> keys : v.next()._2.entrySet()) {
//                    System.out.println(keys.getValue()._1.doubleValue());
//                    System.out.println(keys.getValue()._1);
//                    System.out.println(keys.getValue()._1.toString());
//                    System.out.println(Double.longBitsToDouble(keys.getValue()._1));
//                    System.out.println(Double.parseDouble(keys.getValue()._1.toString()));
//                    System.out.format("%s, %s, %s, %s \n", keys.getKey(), keys.getValue()._1, keys.getValue()._1, Arrays.toString(keys.getValue()._2));
//                    System.out.println(Arrays.toString(keys.getValue()._2));
                    pipelined.zadd(Bytes.toBytes(keys.getKey()), keys.getValue()._1, keys.getValue()._2);
                }
                if (++size % 2000 == 0) {
                    pipelined.sync();
                    System.out.format("[XZOrderingLoader.storeXZSecondaryTable] 存储进度: %d 项%n", size);
                }
            }
            pipelined.sync();
            pipelined.close();
            jedis.close();
        });
    }

    /**
     * 存储主表数据（XZ Ordering 版本）
     * 重写基类方法，使用 XZ Ordering 特定的索引策略
     *
     * @throws IOException 存储过程中的 IO 异常
     */
    @Override
    void storePrimaryTable() throws IOException {
        SparkConf conf = new SparkConf()
//                .setMaster("local[*]")
                .setAppName(XZOrderingLoader.class.getSimpleName() + tableName)
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        conf.registerKryoClasses(new Class[]{Put.class, Tuple2.class});
        createTrajTable();
        deleteSecondaryTable();
        try (JavaSparkContext context = new JavaSparkContext(conf)) {
            JavaRDD<String> rawTrajRDD = context.textFile(sourcePath);
            Configuration hbaseConf = HBaseConfiguration.create();
            JobConf job = new JobConf(hbaseConf);
            job.setOutputFormat(TableOutputFormat.class);
            job.set(TableOutputFormat.OUTPUT_TABLE, tableName);
            JavaRDD<Tuple2<Put, Map<String, Tuple2<Long, byte[]>>>> indexedRDD = rawTrajRDD.map(v -> {
                Tuple2<Put, Long> indexedPut = TrajPutUtil.getPut(v, config);
                Put put = indexedPut._1;
                // 将索引放入统计表中，利用直方图统计表
                Map<String, Tuple2<Long, byte[]>> secondaryIndexMap = getSecondaryIndex(v, put.getRow());
                if (config.getPrimary().equals(SPATIAL) || config.getPrimary().equals(ST)) {
                    secondaryIndexMap.put(tableName, new Tuple2<>(indexedPut._2, put.getRow()));
                }
                return new Tuple2<>(put, secondaryIndexMap);
            }).repartition(200);
            if (config.getPrimary().equals(SPATIAL) || config.getPrimary().equals(ST)) {
                indexedRDD.mapToPair(v -> new Tuple2<>(v._2.get(tableName)._1, 1)).reduceByKey((Function2<Integer, Integer, Integer>) Integer::sum)
                        .foreachPartition(v -> {
                            int size = 0;
                            Jedis jedis = new Jedis(config.getRedisHost(), 6379);
                            Pipeline pipelined = jedis.pipelined();
                            while (v.hasNext()) {
                                Tuple2<Long, Integer> indexValue = v.next();
                                Long index = indexValue._1;
//                                System.out.format("%s, %s, %s\n",index, Double.valueOf(indexValue._1), indexValue._2);
//                                pipelined.zadd(Bytes.toBytes(tableName), index, Bytes.toBytes(indexValue._2));
                                byte[] indexedSize = new byte[12];
                                ByteArraysWrapper.writeInt(indexValue._2, indexedSize, 0);
                                ByteArraysWrapper.writeLong(index, indexedSize, 4);
                                pipelined.zadd(Bytes.toBytes(tableName), index, indexedSize);
//                                System.out.format("%s, %s\n", Double.longBitsToDouble(index), indexValue._2);
                                if (++size % 2000 == 0) {
                                    pipelined.sync();
                                    System.out.format("[XZOrderingLoader.storePrimaryTable] Redis 存储进度: %d 项%n", size);
                                }
                            }
                            pipelined.sync();
                            pipelined.close();
                            jedis.close();
                        });
            }
            indexedRDD.mapToPair(v -> (new Tuple2<>(new ImmutableBytesWritable(), v._1))).saveAsHadoopDataset(job);
            System.out.println("[XZOrderingLoader.storePrimaryTable] 主表存储完成");
            storeXZSecondaryTable(indexedRDD);
            System.out.println("[XZOrderingLoader.storePrimaryTable] 二级索引表存储完成");
        }
    }
}
