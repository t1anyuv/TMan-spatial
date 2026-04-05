# TMan-Spatial

## 简介

TMan-Spatial 是基于 [TMan](https://ieeexplore.ieee.org/document/10184591) 的轨迹数据管理系统的空间查询实现版本。本系统专注于实现空间范围查询功能，并支持多种空间填充曲线索引方法。

TMan (Trajectory Management System) 是一个基于键值存储的高性能轨迹数据管理系统。本项目实现了其核心的空间索引和查询功能，支持：

- 空间范围查询（Spatial Range Query）
- 多种空间填充曲线：Z-Order、XZ-Order、LocS 等
- 基于 HBase 的分布式存储
- 基于 Redis 的索引缓存
- TSP 编码优化和自适应分区

**参考文献：** [TMan: A High-Performance Trajectory Data Management System Based on Key-Value Stores | IEEE Conference Publication](https://ieeexplore.ieee.org/document/10184591)

## 环境要求

### 基础环境
- Java 8 或更高版本
- Scala 2.11
- Maven 3.x
- Apache Spark 2.3.3
- Apache HBase 2.1.2
- Redis 服务器

### 主要依赖
- HBase Client/Server 2.1.2
- Spark Core 2.3.3
- Scala 2.11.12
- Redis Jedis 4.3.1
- JTS Core 1.16.0（空间几何处理）
- LocationTech SFCurve 0.2.0（空间填充曲线）
- Protocol Buffers 2.5.0
- gRPC 1.6.1

### 系统要求
- 操作系统：Linux/Windows/macOS
- 内存：建议 8GB 以上
- 存储：根据数据规模而定

## 如何运行与使用

### 1. 环境配置

#### 启动 HBase
```bash
# 启动 HBase 服务
start-hbase.sh
```

#### 启动 Redis
```bash
# 启动 Redis 服务（默认端口 6379）
redis-server
```

### 2. 编译项目

```bash
# 使用 Maven 编译项目
mvn clean package

# 生成带依赖的 JAR 包
mvn assembly:single
```

编译完成后，会在 `target` 目录下生成 `TMan-spatial-1.0-SNAPSHOT-jar-with-dependencies.jar`。

### 3. 数据存储

系统提供了两种主要的数据加载方式：

#### 使用 XZ-Order 索引存储
```bash
java -cp target/TMan-spatial-1.0-SNAPSHOT-jar-with-dependencies.jar \
  experiments.tman.TMANStoring \
  <数据源路径> <表名> <结果路径>
```

#### 使用 LETI 索引存储
```bash
java -cp target/TMan-spatial-1.0-SNAPSHOT-jar-with-dependencies.jar \
  experiments.tman.LetiStoring \
  <sourcePath> <resolution> <alpha> <beta> <timeBin> <timeBinNums> \
  <compressType> <xmin> <ymin> <xmax> <ymax> <shards> <pIndex> \
  <tableName> <redisHost> <adaptivePartition:0|1> <resultPath>
```

**参数说明：**
- `adaptivePartition`：是否开启自适应划分（`0` 关闭，`1` 开启）

### 4. 空间范围查询

#### 使用 XZ-Order 索引查询
```bash
java -cp target/TMan-spatial-1.0-SNAPSHOT-jar-with-dependencies.jar \
  experiments.tman.SpatialQuery \
  <表名> <查询条件文件> <结果路径>
```

#### 使用 LETI 索引查询
```bash
java -cp target/TMan-spatial-1.0-SNAPSHOT-jar-with-dependencies.jar \
  experiments.tman.LetiSpatialQuery \
  <表名> <查询条件文件> <结果路径>
```

**查询条件文件格式：**
每行一个查询窗口，格式为：`minLng,minLat,maxLng,maxLat`

### 5. 配置说明

系统的主要配置通过 `TableConfig` 类进行设置：

```java
TableConfig config = new TableConfig(
    IndexEnum.INDEX_TYPE.SPATIAL,  // 索引类型
    10,                             // 分辨率
    4,                              // alpha（网格宽度）
    4,                              // beta（网格高度）
    3600.0,                         // 时间分箱大小
    24,                             // 时间分箱数量
    IIntegerCompress.CompressType.NONE,  // 压缩类型
    envelope,                       // 空间范围
    shards                          // 分片数
);

// 设置 Redis 主机
config.setRedisHost("127.0.0.1");

// 启用 XZ-Order 索引
config.setIsXZ(1);

// 启用 TSP 编码
config.setTspEncoding(1);
```

## 其他信息

### 项目结构

```
src/main/
├── java/
│   ├── client/          # HBase 客户端
│   ├── config/          # 配置类
│   ├── entity/          # 实体类
│   ├── experiments/     # 实验代码（存储和查询）
│   ├── filter/          # 空间过滤器
│   ├── loader/          # 数据加载器
│   ├── preprocess/      # 数据预处理和压缩
│   ├── query/           # 查询规划器
│   └── utils/           # 工具类
├── scala/
│   ├── index/           # 空间索引实现（XZ、LocS 等）
│   └── utils/           # Scala 工具类
├── proto/               # Protocol Buffers 定义
└── resources/           # 配置文件
```

### 支持的空间填充曲线

1. **Z-Order Curve**：经典的 Z 曲线索引
2. **XZ-Order Curve**：扩展的 XZ 曲线索引
3. **LocS Index**：基于位置和形状的索引
4. **LETI Index**：LETI 空间索引

### 核心特性

- **空间索引**：支持多种空间填充曲线，提供高效的空间范围查询
- **分布式存储**：基于 HBase 的分布式键值存储
- **索引缓存**：使用 Redis 缓存索引信息，加速查询
- **TSP 编码**：支持旅行商问题（TSP）编码优化，减少存储空间
- **自适应分区**：支持自适应空间分区，提高查询效率
- **压缩支持**：支持多种整数压缩算法（Delta、PFOR、Simple8b 等）

### 数据格式

输入数据格式为：`oid-time-wkt`

示例：
```
1-1609459200-LINESTRING(116.3 39.9, 116.4 40.0, 116.5 40.1)
2-1609459260-LINESTRING(116.2 39.8, 116.3 39.9, 116.4 40.0)
```