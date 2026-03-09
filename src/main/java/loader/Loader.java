package loader;

import com.esri.core.geometry.*;
import config.TableConfig;
import utils.ByteArraysWrapper;
import utils.TrajPutUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapred.TableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import scala.Tuple2;
import scala.Tuple3;

import java.io.Closeable;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;

import static client.Constants.*;
import static constans.IndexEnum.INDEX_TYPE.*;
import static utils.TSPGreedy.encodeShapes;
import static utils.TrajPutUtil.getPutWithIndex;
import static utils.TrajPutUtil.getSpatialIndex;


public class Loader implements Closeable, Serializable {
    final TableConfig config;
    final String tableName;
    final String sourcePath;

    final String resultPath;

    final transient Admin admin;

    final transient Connection connection;

    public Loader(TableConfig config, String tableName, String sourcePath, String resultPath) throws IOException {
        this.config = config;
        this.tableName = tableName;
        this.sourcePath = sourcePath;
        Configuration conf = HBaseConfiguration.create();
        this.connection = ConnectionFactory.createConnection(conf);
        this.admin = connection.getAdmin();
        this.resultPath = resultPath;
    }

    public Loader(TableConfig config, String sourcePath, String resultPath) throws IOException {
        this(config, config.getTableName(), sourcePath, resultPath);
    }

    /**
     * 执行完整的数据存储流程
     * 包括：元数据表、统计表、主表和二级索引表的存储
     *
     * @throws IOException 存储过程中的 IO 异常
     */
    public void store() throws IOException {
        System.out.println("[Loader] ========== 开始存储数据 ==========");
        System.out.println("[Loader] 步骤 1/4: 存储元数据表...");
        storeMetaTable();
        System.out.println("[Loader] 步骤 2/4: 存储统计表...");
        storeStatTable();
        System.out.println("[Loader] 步骤 3/4: 存储主表...");
        storePrimaryTable();
        System.out.println("[Loader] ========== 数据存储完成 ==========");
//        storeSecondaryTable();
    }

    /**
     * 存储二级索引表到 Redis
     * 从 indexedRDD 中提取二级索引信息，并写入 Redis 的有序集合中
     *
     * @param indexedRDD 包含 Put 操作、二级索引映射和 KeyValue 列表的 RDD
     */
    void storeSecondaryTable(JavaRDD<Tuple3<Put, Map<String, Tuple2<Long, byte[]>>, List<KeyValue>>> indexedRDD) {
        indexedRDD.map(v -> {
            v._2().remove(tableName);
            return v;
        }).foreachPartition(v -> {
            int partitionSize = 0;
            int totalIndexEntries = 0;
            Jedis jedis = new Jedis(config.getRedisHost(), 6379, 60000);
            Pipeline pipelined = jedis.pipelined();

            System.out.println("[Loader.storeSecondaryTable] 开始存储二级索引条目...");

            while (v.hasNext()) {
                Tuple3<Put, Map<String, Tuple2<Long, byte[]>>, List<KeyValue>> tuple = v.next();
                partitionSize++;

                for (Map.Entry<String, Tuple2<Long, byte[]>> entry : tuple._2().entrySet()) {
                    String indexTableName = entry.getKey();
                    Long indexValue = entry.getValue()._1();
                    byte[] rowKey = entry.getValue()._2();

                    // 打印详细信息：索引表名、索引值、行键
                    // System.out.format("[Loader.storeSecondaryTable] 索引表: %s, 索引值: %d (0x%x), 行键: %s%n",
                    //         indexTableName, indexValue, indexValue, Bytes.toStringBinary(rowKey));

                    pipelined.zadd(Bytes.toBytes(indexTableName), indexValue, rowKey);
                    totalIndexEntries++;
                }

                if (partitionSize % 2000 == 0) {
                    pipelined.sync();
                    System.out.format("[Loader.storeSecondaryTable] 进度: 已处理 %d 条轨迹, 已存储 %d 个索引条目%n",
                            partitionSize, totalIndexEntries);
                }
            }

            pipelined.sync();
            pipelined.close();
            jedis.close();

            System.out.format("[Loader.storeSecondaryTable] 分区完成: 处理了 %d 条轨迹, 存储了 %d 个索引条目%n",
                    partitionSize, totalIndexEntries);
        });
    }

    /**
     * 获取轨迹的二级索引映射
     * <p>
     *
     *
     * @param traj   轨迹字符串，格式为 "oid-time-wkt"
     * @param rowKey 主表的行键
     * @return 二级索引映射，key 为索引表名，value 为 (索引值, 行键) 的元组
     */
    /**
     * 获取轨迹的二级索引映射
     * <p>
     * 当前版本仅支持空间主索引，不需要二级索引。
     *
     * @param traj   轨迹字符串，格式为 "oid-time-wkt"
     * @param rowKey 主表的行键
     * @return 空的二级索引映射
     */
    Map<String, Tuple2<Long, byte[]>> getSecondaryIndex(String traj, byte[] rowKey) {
        // 断言：当前版本仅支持空间主索引
        assert config.getPrimary().equals(SPATIAL) : "Only SPATIAL primary index is supported";

        // 空间是主索引，不需要其他二级索引
        return new HashMap<>();
    }

    /**
     * 删除 Redis 中的二级索引表
     * <p>
     * 在存储新数据前清理旧的索引数据。
     * 当前版本仅支持空间主索引，因此只删除 OID 二级索引和统计表。
     */
    /**
     * 删除 Redis 中的二级索引表
     * <p>
     * 在存储新数据前清理旧的索引数据。
     * 当前版本仅支持空间主索引，只需删除统计表。
     */
    public void deleteSecondaryTable() {
        // 断言：当前版本仅支持空间主索引
        assert config.getPrimary().equals(SPATIAL) : "Only SPATIAL primary index is supported";

        Jedis jedis = new Jedis(config.getRedisHost(), 6379, 60000);
        Pipeline pipelined = jedis.pipelined();

        // 删除统计表
        pipelined.del(tableName);

        pipelined.sync();
        pipelined.close();
        jedis.close();
    }

    /**
     * 存储统计表
     * 当前实现为空，子类可以重写此方法实现统计表的存储
     */
    void storeStatTable() {
    }

    /**
     * 存储主表数据
     * 使用 Spark 读取源数据，生成索引，并写入 HBase 主表和 Redis 索引
     *
     * @throws IOException 存储过程中的 IO 异常
     */
    void storePrimaryTable() throws IOException {
        SparkConf conf = new SparkConf()
                .setMaster("local[*]")
                .setAppName(Loader.class.getSimpleName() + tableName)
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        conf.registerKryoClasses(new Class[]{Put.class, Tuple2.class});

        createTrajTable();
        deleteSecondaryTable();

        long currentTime = System.currentTimeMillis();

        try (JavaSparkContext context = new JavaSparkContext(conf)) {
            JavaRDD<String> rawTrajRDD = context.textFile(sourcePath);

            // ========================================================================================================
            // indexedRDD: <Put, secondaryIndexMap, list of KV>
            // - Put: HBase Put operation for the primary table
            // - secondaryIndexMap: Map of secondary index table names to their corresponding index values and row keys
            // - list of KV: List of HBase KeyValue objects for bulk loading
            // ========================================================================================================
            JavaRDD<Tuple3<Put, Map<String, Tuple2<Long, byte[]>>, List<KeyValue>>> indexedRDD;

            // 判断是否需要编码
            boolean needTSPEncoding = config.isTspEncoding() && config.getPrimary().equals(SPATIAL);

            if (needTSPEncoding) {
                indexedRDD = rawTrajRDD.mapToPair(v -> {
                    // 获取空间索引: level, quadCode, shapeCode
                    Tuple3<Object, Object, Object> indexValue = getSpatialIndex(v, config);
                    long quadCode = (long) indexValue._2();
                    long shapeCode = (long) indexValue._3();
                    return new Tuple2<>(quadCode, new Tuple2<>(shapeCode, v));
                }).groupByKey().flatMap(v -> {
                    Set<Long> shapes = new HashSet<>();
                    List<Tuple2<Long, String>> trajs = new ArrayList<>();
                    // v: quad code, list of (shapeId, traj)
                    for (Tuple2<Long, String> value : v._2) {
                        shapes.add(value._1);
                        trajs.add(value);
                    }

                    Long quadCode = v._1();

                    // 编码 shape
                    List<Long> shapeList = new ArrayList<>(shapes);
                    List<Integer> shapeOrders = encodeShapes(shapeList, config.getTspEncoding());

                    // 生成 Put 操作
                    List<Tuple3<Put, Map<String, Tuple2<Long, byte[]>>, List<KeyValue>>> puts = new ArrayList<>();
                    for (Tuple2<Long, String> traj : trajs) {
                        // 计算 sOrder：如果使用 TSP Encoding，使用编码后的值；否则使用原始 shapeId
                        int sOrder;
                        Long trajShape = traj._1;
                        String trajStr = traj._2;
                        sOrder = shapeOrders.indexOf(shapeList.indexOf(trajShape));

                        // 使用 quad code 和 shape order 构造 Put
                        // Put: [shard(1)][index(8)][tid]
                        Tuple3<Put, Long, List<KeyValue>> put = getPutWithIndex(trajStr, quadCode, sOrder, config);
                        // 完成二级索引到主表 row key 的映射
                        Map<String, Tuple2<Long, byte[]>> secondaryIndexMap = getSecondaryIndex(trajStr, put._1().getRow());

                        if (config.getPrimary().equals(SPATIAL) || config.getPrimary().equals(ST)) {
                            // 使用原始 shape code 和 quad code
                            long index = trajShape | (quadCode << ((long) config.getAlpha() * (long) config.getBeta()));
                            // ===========================================================
                            // secondaryIndexMap: tableName: <origin_index, encoded_order>
                            // ===========================================================
                            secondaryIndexMap.put(tableName, new Tuple2<>(index, Bytes.toBytes(sOrder)));
                        }

                        puts.add(new Tuple3<>(put._1(), secondaryIndexMap, put._3()));
                    }
                    return puts.iterator();
                });
            } else {
                // 不使用 TSP 编码
                indexedRDD = rawTrajRDD.map(v -> {
                    Tuple2<Put, Long> indexedPut = TrajPutUtil.getPut(v, config);
                    Put put = indexedPut._1;
//                System.out.println(Arrays.toString(put.getRow()));
                    // TODO: 将索引放入统计表中，利用直方图统计表
                    Map<String, Tuple2<Long, byte[]>> secondaryIndexMap = getSecondaryIndex(v, put.getRow());
                    if (config.getPrimary().equals(SPATIAL) || config.getPrimary().equals(ST)) {
//                    System.out.println(indexedPut._2);
                        secondaryIndexMap.put(tableName, new Tuple2<>(indexedPut._2, put.getRow()));
                    }
                    List<KeyValue> t = new ArrayList<>();
                    return new Tuple3<>(put, secondaryIndexMap, t);
                });
            }
            indexedRDD.persist(StorageLevel.DISK_ONLY());
            if (config.getPrimary().equals(SPATIAL) || config.getPrimary().equals(ST)) {
                // v._2().get(tableName)._1: origin_index
                // v._2().get(tableName)._2: encoded_order
                indexedRDD
                        // (origin_index, (1, encoded_order))
                        .mapToPair(v -> new Tuple2<>(v._2().get(tableName)._1, new Tuple2<>(1, v._2().get(tableName)._2)))
                        // (count, encoded_order)
                        .reduceByKey((v1, v2) -> new Tuple2<>(v1._1() + v2._1, v1._2))
//                        .repartition(100)
                        .foreachPartition(v -> {
                            int size = 0;
                            Jedis jedis = new Jedis(config.getRedisHost(), 6379, 60000);
                            Pipeline pipelined = jedis.pipelined();
                            while (v.hasNext()) {
//                        Tuple2<Long, Integer> indexValue = v.next();
                                Tuple2<Long, Tuple2<Integer, byte[]>> indexValue = v.next();
                                Long index = indexValue._1;
                                byte[] indexedSize = new byte[16];
//                        ByteArrays.writeInt(indexValue._2, indexedSize, 0);

                                // ===============================================================
                                // ZSET: key: tableName, score: origin_index, value: indexedSize
                                // indexedSize: count(0-4), encoded_order(4-8), origin_index(8-16)
                                // ===============================================================
                                ByteArraysWrapper.writeInt(indexValue._2._1, indexedSize, 0); // count
                                System.arraycopy(indexValue._2._2, 0, indexedSize, 4, 4); // encoded_order
                                ByteArraysWrapper.writeLong(index, indexedSize, 8); // origin_index
                                pipelined.zadd(Bytes.toBytes(tableName), index, indexedSize);
//                                System.out.format("%s, %s\n", Double.longBitsToDouble(index), indexValue._2);
                                if (++size % 2000 == 0) {
                                    pipelined.sync();
                                    System.out.printf("[Loader.storePrimaryTable] Redis 存储进度: %6d 项%n", size);
                                }
                            }
//                            System.out.println("size: " + size);
                            pipelined.sync();
                            pipelined.sync();
                            pipelined.close();
                            jedis.close();
                        });
            }

            // 使用 saveAsHadoopDataset 模式存储
            storePrimaryTableWithHadoopDataset(indexedRDD);

            // storePrimaryTableWithBulkLoad(indexedRDD);

            // storePrimaryTableWithNewAPIHadoopDataset(indexedRDD);

            currentTime = System.currentTimeMillis() - currentTime;
            String path = resultPath + "indexing_time_" + tableName;
            System.out.println("[Loader.storePrimaryTable] 索引时间文件路径: " + path);
            FileWriter writer = new FileWriter(path);
            writer.write("indexing time: " + currentTime);
            writer.flush();
            writer.close();
            System.out.println("[Loader.storePrimaryTable] 主表存储完成，耗时: " + currentTime + " ms");
            storeSecondaryTable(indexedRDD);
            System.out.println("[Loader.storePrimaryTable] 二级索引表存储完成");
        }
    }

    /**
     * 存储模式1: 使用 HFileOutputFormat2 进行 Bulk Load
     * 这种方式通过生成 HFile 文件然后批量加载到 HBase，性能较好但需要额外的临时存储空间
     *
     * @param indexedRDD 包含 Put 操作、二级索引映射和 KeyValue 列表的 RDD
     * @throws IOException 存储过程中的 IO 异常
     */
    private void storePrimaryTableWithBulkLoad(JavaRDD<Tuple3<Put, Map<String, Tuple2<Long, byte[]>>, List<KeyValue>>> indexedRDD) throws IOException {
        Configuration hbaseConf = HBaseConfiguration.create();
        hbaseConf.set(TableOutputFormat.OUTPUT_TABLE, tableName);
        hbaseConf.setInt("hbase.mapreduce.bulkload.max.hfiles.perRegion.perFamily", 3200);
        // hbaseConf.set("hbase.hregion.max.filesize", "3200");

        Connection conn = ConnectionFactory.createConnection(hbaseConf);
        Admin admin = conn.getAdmin();
        Table table = conn.getTable(TableName.valueOf(tableName));

        Job job = Job.getInstance(hbaseConf);
        // 设置job的输出格式
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(KeyValue.class);
        job.setOutputFormatClass(HFileOutputFormat2.class);
        HFileOutputFormat2.configureIncrementalLoad(job, table, conn.getRegionLocator(TableName.valueOf(tableName)));

        FileSystem fs = FileSystem.newInstance(new Configuration());
        String tmpTable = "file:///tmp/hbase" + tableName;
        if (fs.exists(new Path(tmpTable))) {
            fs.delete(new Path(tmpTable), true);
        }

        // TODO: put to key-value
        indexedRDD.flatMap(v -> v._3().iterator())
                .mapToPair(v -> new Tuple2<>(v, 1))
                .sortByKey(new KeyValueOrder())
                .repartition(200)
                .mapToPair(v -> (new Tuple2<>(new ImmutableBytesWritable(v._1.getKey()), v._1)))
                .saveAsNewAPIHadoopFile(tmpTable, ImmutableBytesWritable.class, KeyValue.class, HFileOutputFormat2.class, job.getConfiguration());

        @SuppressWarnings("deprecation")
        LoadIncrementalHFiles bulkLoader = new LoadIncrementalHFiles(job.getConfiguration());
        bulkLoader.doBulkLoad(new Path(tmpTable), admin, table, conn.getRegionLocator(table.getName()));

        if (fs.exists(new Path(tmpTable))) {
            fs.delete(new Path(tmpTable), true);
        }

        admin.close();
        table.close();
        conn.close();
    }

    /**
     * 存储模式2: 使用 saveAsNewAPIHadoopDataset
     * 这种方式使用新的 Hadoop API，支持写入到临时文件系统
     *
     * @param indexedRDD 包含 Put 操作、二级索引映射和 KeyValue 列表的 RDD
     * @throws IOException 存储过程中的 IO 异常
     */
    private void storePrimaryTableWithNewAPIHadoopDataset(JavaRDD<Tuple3<Put, Map<String, Tuple2<Long, byte[]>>, List<KeyValue>>> indexedRDD) throws IOException {
        Configuration hbaseConf = HBaseConfiguration.create();
        JobConf job = new JobConf(hbaseConf);
        job.setOutputFormat(TableOutputFormat.class);
        job.setOutputKeyClass(ImmutableBytesWritable.class);
        job.setOutputValueClass(Put.class);
        job.set(TableOutputFormat.OUTPUT_TABLE, tableName);

        String tmpTable = "file:///tmp/hbase" + tableName;
        FileSystem fs = FileSystem.newInstance(new Configuration());
        if (fs.exists(new Path(tmpTable))) {
            fs.delete(new Path(tmpTable), true);
        }
        // FileInputFormat.setInputPaths(job, new Path(tmpTable));
        FileOutputFormat.setOutputPath(job, new Path(tmpTable));

        indexedRDD.mapToPair(v -> (new Tuple2<>(new ImmutableBytesWritable(), v._1()))).saveAsNewAPIHadoopDataset(job);
    }

    /**
     * 存储模式3: 使用 saveAsHadoopDataset（当前使用的模式）
     * 这种方式直接写入 HBase 表，简单直接但可能性能不如 Bulk Load
     *
     * @param indexedRDD 包含 Put 操作、二级索引映射和 KeyValue 列表的 RDD
     */
    void storePrimaryTableWithHadoopDataset(JavaRDD<Tuple3<Put, Map<String, Tuple2<Long, byte[]>>, List<KeyValue>>> indexedRDD) {
        Configuration hbaseConf = HBaseConfiguration.create();
        JobConf job = new JobConf(hbaseConf);
        job.setOutputFormat(TableOutputFormat.class);
        job.setOutputKeyClass(ImmutableBytesWritable.class);
        job.setOutputValueClass(Put.class);
        job.set(TableOutputFormat.OUTPUT_TABLE, tableName);

        indexedRDD.mapToPair(v -> (new Tuple2<>(new ImmutableBytesWritable(), v._1()))).saveAsHadoopDataset(job);
    }

    /**
     * 获取 HBase 表对象
     *
     * @param tableName 表名
     * @return HBase 表对象
     * @throws IOException 获取表对象时的 IO 异常
     */
    private Table getTable(String tableName) throws IOException {
        return this.connection.getTable(TableName.valueOf(tableName));
    }

    /**
     * 创建轨迹表
     * 如果表已存在，先删除再创建
     *
     * @throws IOException 创建表时的 IO 异常
     */
    public void createTrajTable() throws IOException {
        ColumnFamilyDescriptor cf = ColumnFamilyDescriptorBuilder.of(DEFAULT_CF);

        TableDescriptor table = TableDescriptorBuilder.newBuilder(TableName.valueOf(tableName))
                .setColumnFamily(cf)
                .build();
        if (admin.tableExists(table.getTableName())) {
            if (!this.admin.isTableDisabled(table.getTableName())) {
                this.admin.disableTable(table.getTableName());
            }
            this.admin.deleteTable(table.getTableName());
        }
        this.admin.createTable(table);
    }

    /**
     * 存储元数据表
     * 将表配置信息（索引类型、参数等）存储到 HBase 元数据表中
     *
     * @throws IOException 存储过程中的 IO 异常
     */
    public void storeMetaTable() throws IOException {
        String name = tableName + META_TABLE;
        ColumnFamilyDescriptor cf = ColumnFamilyDescriptorBuilder.of(DEFAULT_CF);

        TableDescriptor table = TableDescriptorBuilder.newBuilder(TableName.valueOf(name))
                .setColumnFamily(cf)
                .build();
        if (admin.tableExists(table.getTableName())) {
            if (!this.admin.isTableDisabled(table.getTableName())) {
                this.admin.disableTable(table.getTableName());
            }
//            this.admin.disableTable(table.getTableName());
            this.admin.deleteTable(table.getTableName());
        }
        this.admin.createTable(table);
        Table hTable = this.connection.getTable(TableName.valueOf(name));
        // row key
        Put put = new Put(Bytes.toBytes(META_TABLE_ROWKEY));
        if (null != config.getShards()) {
            put.addColumn(Bytes.toBytes(DEFAULT_CF), Bytes.toBytes(META_TABLE_SHARDS), Bytes.toBytes(config.getShards()));
        }
        put.addColumn(Bytes.toBytes(DEFAULT_CF), Bytes.toBytes(META_TABLE_PRIMARY), Bytes.toBytes(config.getPrimary().name()));
        put.addColumn(Bytes.toBytes(DEFAULT_CF), Bytes.toBytes(META_TABLE_ALPHA), Bytes.toBytes(config.getAlpha()));
        put.addColumn(Bytes.toBytes(DEFAULT_CF), Bytes.toBytes(META_TABLE_RESOLUTION), Bytes.toBytes(config.getResolution()));
        put.addColumn(Bytes.toBytes(DEFAULT_CF), Bytes.toBytes(META_TABLE_BETA), Bytes.toBytes(config.getBeta()));
        put.addColumn(Bytes.toBytes(DEFAULT_CF), Bytes.toBytes(META_TABLE_TIME_BIN), Bytes.toBytes(config.getTimeBin()));
        put.addColumn(Bytes.toBytes(DEFAULT_CF), Bytes.toBytes(META_TABLE_TIME_BIN_NUMS), Bytes.toBytes(config.getTimeBinNums()));
        put.addColumn(Bytes.toBytes(DEFAULT_CF), Bytes.toBytes(META_TABLE_compression), Bytes.toBytes(config.getCompressType().name()));
        put.addColumn(Bytes.toBytes(DEFAULT_CF), Bytes.toBytes(META_REDIS), Bytes.toBytes(config.getRedisHost()));
        put.addColumn(Bytes.toBytes(DEFAULT_CF), Bytes.toBytes(META_TABLE_IS_XZ), Bytes.toBytes(config.getIsXZ()));
        put.addColumn(Bytes.toBytes(DEFAULT_CF), Bytes.toBytes(META_TABLE_IS_TSP_ENCODING), Bytes.toBytes(config.getTspEncoding()));
        put.addColumn(Bytes.toBytes(DEFAULT_CF), Bytes.toBytes(META_TABLE_IS_RL_ENCODING), Bytes.toBytes(config.getRlEncoding()));
        put.addColumn(Bytes.toBytes(DEFAULT_CF), Bytes.toBytes(META_TABLE_ADAPTIVE_PARTITION), Bytes.toBytes(config.getAdaptivePartition()));
        put.addColumn(Bytes.toBytes(DEFAULT_CF), Bytes.toBytes(META_TABLE_MAX_SHAPE_BITS), Bytes.toBytes(config.getMaxShapeBits()));
        Envelope envelope = config.getEnvelope();
        if (null != envelope) {
            put.addColumn(Bytes.toBytes(DEFAULT_CF), Bytes.toBytes(META_TABLE_xmin), Bytes.toBytes(envelope.getXMin()));
            put.addColumn(Bytes.toBytes(DEFAULT_CF), Bytes.toBytes(META_TABLE_xmax), Bytes.toBytes(envelope.getXMax()));
            put.addColumn(Bytes.toBytes(DEFAULT_CF), Bytes.toBytes(META_TABLE_ymin), Bytes.toBytes(envelope.getYMin()));
            put.addColumn(Bytes.toBytes(DEFAULT_CF), Bytes.toBytes(META_TABLE_ymax), Bytes.toBytes(envelope.getYMax()));
        }

        hTable.put(put);
    }

    @Override
    public void close() throws IOException {
        this.admin.close();
        this.connection.close();
    }

    /**
     * KeyValue 排序比较器
     * 用于 Bulk Load 时对 KeyValue 进行排序
     * 比较顺序：Row Key -> Column Family -> Column Qualifier -> Value
     */
    public static class KeyValueOrder implements Comparator<KeyValue>, Serializable {
        @Override
        public int compare(KeyValue x, KeyValue y) {
            // 比较 Row Key
            byte[] xRow = CellUtil.cloneRow(x);
            byte[] yRow = CellUtil.cloneRow(y);
            int com = Bytes.compareTo(xRow, yRow);
            if (com != 0) return com;

            // 比较 Column Family
            byte[] xf = CellUtil.cloneFamily(x);
            byte[] yf = CellUtil.cloneFamily(y);
            com = Bytes.compareTo(xf, yf);
            if (com != 0) return com;

            // 比较 Column Qualifier
            byte[] xq = CellUtil.cloneQualifier(x);
            byte[] yq = CellUtil.cloneQualifier(y);
            com = Bytes.compareTo(xq, yq);
            if (com != 0) return com;

            // 比较 Value
            byte[] xv = CellUtil.cloneValue(x);
            byte[] yv = CellUtil.cloneValue(y);
            return Bytes.compareTo(xv, yv);
        }
    }
}
