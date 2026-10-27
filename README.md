# 广西特色农产品电商订单流批一体分析系统

基于 Spark Structured Streaming + Kafka + Hive 四层数仓的广西本地农产品电商数据分析系统。
一条链路覆盖 **数据采集 → 消息传输 → 离线数仓 → 实时计算 → 指标入库 → 可视化大屏**，
并附带品类销量预测与品类画像聚类。

## 一、项目简介

面向广西特色农产品（砂糖橘、武鸣沃柑、百色芒果、北流百香果、柳州螺蛳粉等）的线上订单场景，
解决三个实际问题：订单分散在多环节缺少统一口径看板、按天出数看不到当天走势、刷单与脏数据污染成交额。

系统的核心是 **流批一体**：同一份计算逻辑（`process_batch`），既能作为流任务持续消费 Kafka，
也能作为批任务重跑历史数据，做到"一套代码，两种跑法"。

## 二、系统架构

```
┌──────────────────────────────────────────────────────────────┐
│  数据生产层   kafka_producer.py                              │
│  模拟订单 JSON 流（含异常单 / 重复单号 / 迟到数据注入）        │
└───────────────────────────┬──────────────────────────────────┘
                            │ JSON
┌───────────────────────────▼──────────────────────────────────┐
│  消息传输层   Kafka 3.2.1  Topic: guangxi-eshop-stream        │
└───────────────────────────┬──────────────────────────────────┘
             ┌──────────────┴───────────────┐
┌────────────▼─────────────┐   ┌────────────▼─────────────────┐
│ 实时计算层                │   │ 离线数仓层（Hive）            │
│ Spark Structured Streaming│   │ ODS → DWD → DWS → ADS       │
│ Watermark 60s             │   │ SparkSQL 离线 ETL            │
│ Checkpoint（HDFS）        │   │ 分区裁剪 / ORC + Snappy      │
│ ForeachBatch 流批复用      │   └────────────┬─────────────────┘
└────────────┬─────────────┘                │
             │ 实时指标                     │ 离线日报
┌────────────▼──────────────────────────────▼─────────────────┐
│  存储层   MySQL 8.0（gx_ec_mysql）+ HDFS + Hive（gx_ec_hive） │
└───────────────────────────┬──────────────────────────────────┘
                            │
┌───────────────────────────▼──────────────────────────────────┐
│  应用层   Flask（/api/eshop_data）+ ECharts 大屏（3s 刷新）    │
└──────────────────────────────────────────────────────────────┘
```

## 三、技术栈版本

| 组件 | 版本 | 用途 |
| --- | --- | --- |
| CentOS | 7.9 x64 | 虚拟机操作系统（伪分布式） |
| JDK | 1.8.0_202 | 全部大数据组件运行时 |
| Hadoop | 3.3.4 | HDFS 存储 + YARN 资源调度 |
| Hive | 3.1.2 | 四层离线数仓（MySQL 作元数据库） |
| Spark | 3.3.0 (bin-hadoop3) | SparkSQL 离线 ETL + Structured Streaming |
| Kafka | 3.2.1 (Scala 2.12) | 实时消息队列（内置 Zookeeper） |
| MySQL | 8.0.46 | 业务指标库 + Hive 元数据库 |
| Python | 3.9 (Miniconda) | 生产者 / Flask / 机器学习 |
| ECharts | 5.4.3 | 可视化大屏 |

## 四、目录结构

```
.
├── src/
│   ├── kafka_producer.py     # 订单生产者（异常/重复单/迟到数据注入）
│   ├── spark_streaming.py    # 流批一体主任务（水印 + Checkpoint + ForeachBatch）
│   ├── offline_etl.py        # SparkSQL 离线数仓 ETL
│   ├── ml_forecast.py        # 品类销量预测 + 品类聚类
│   └── flask_server.py       # Flask 接口
├── sql/
│   ├── hive_ddl.sql          # Hive 四层建表
│   └── mysql_ddl.sql         # MySQL 业务表
├── dashboard/
│   └── dashboard.html        # ECharts 大屏（单文件）
├── scripts/
│   ├── install_env.sh        # 一键安装整套环境
│   ├── boot_all.sh           # 一键拉起全栈（含 Flask 与大屏）
│   ├── start_all.sh          # 只启动大数据组件
│   └── stop_all.sh           # 反向停止
├── docs/
│   ├── 部署手册.md            # 环境部署 + 启动 + 验收 + 故障排查
│   └── 拓展任务说明.txt        # 拓展任务做了什么、效果如何
└── README.md
```

## 五、快速开始

### 5.1 环境准备

完整步骤见 [docs/部署手册.md](docs/部署手册.md)，最简流程：

```bash
# 1) 安装包放到 /opt/software/ 后，一键装环境
sudo bash scripts/install_env.sh

# 2) 启动全部组件（机器重启后用这个，会连 Flask 与大屏一起拉起）
bash scripts/boot_all.sh
```

### 5.2 启动顺序（必须按此顺序，否则会连不上）

```
Zookeeper → Kafka → HDFS → YARN → Hive Metastore → MySQL
    ↓
生产者 → Spark 流任务 → 离线 ETL → 预测模型 → Flask → 打开大屏
```

```bash
# 1) 生产者：持续产生订单流
python src/kafka_producer.py --ip 192.168.100.100 --total 500

# 2) 流任务：消费 Kafka，实时指标写 MySQL
spark-submit --master local[2] src/spark_streaming.py --mode stream
#    只跑一趟积压数据（截图用）：
spark-submit --master local[2] src/spark_streaming.py --mode stream --once
#    批模式重跑历史，验证流批一体：
spark-submit --master local[2] src/spark_streaming.py --mode batch \
    --input /user/hive/warehouse/gx_ec_hive.db/ods_eshop_order

# 3) 离线数仓 ETL
spark-submit --master local[2] src/offline_etl.py --dt all

# 4) 拓展任务：销量预测 + 品类聚类
python src/ml_forecast.py --days 3

# 5) Flask 接口
python src/flask_server.py

# 6) 浏览器打开大屏
#   直接双击 dashboard/dashboard.html，或
python -m http.server 8080 --directory dashboard   # 然后访问 http://localhost:8080/dashboard.html
```

### 5.3 需要修改的配置项

| 位置 | 配置项 | 默认值 |
| --- | --- | --- |
| `src/kafka_producer.py` | `DEFAULT_IP` | 192.168.100.100 |
| `src/spark_streaming.py` | `CONFIG.kafka_bootstrap` / `CONFIG.mysql` | 192.168.100.100:9092 / :3306 |
| `src/offline_etl.py` | `CONFIG.mysql` | 同上 |
| `src/flask_server.py` | `DB_CONFIG` | gx_ec_mysql / root / 123456 |
| `src/ml_forecast.py` | `DB_CONFIG` | 同上 |
| `scripts/install_env.sh` | `MYSQL_ROOT_PWD` | 123456 |

## 六、验收方法

| 验收项 | 命令 / 地址 | 期望结果 |
| --- | --- | --- |
| HDFS | http://192.168.100.100:9870 | Overview 正常，Live Nodes = 1 |
| YARN | http://192.168.100.100:8088 | Cluster 页面正常 |
| Kafka Topic | `kafka-topics.sh --list --bootstrap-server master:9092` | 有 `guangxi-eshop-stream` |
| 消费验证 | `kafka-console-consumer.sh --topic guangxi-eshop-stream --from-beginning --bootstrap-server master:9092` | 能看到订单 JSON |
| Hive 四层 | `hive -e "use gx_ec_hive; show tables;"` | 6 张表 |
| Hive 数据 | `hive -e "select count(*) from gx_ec_hive.dwd_eshop_order_valid"` | > 0 |
| MySQL 实时表 | `mysql -uroot -p -e "select * from gx_ec_mysql.realtime_order_index"` | 单行累计值在增长 |
| Flask 健康 | http://127.0.0.1:5000/api/health | `{"status":"ok"}` |
| 大屏 | 打开 `dashboard/dashboard.html` | 4 卡片 + 4 图有数据，3 秒刷新 |
| 异常告警 | 大屏右下角列表 | 出现"负金额/地址为空/ID缺失" |

## 七、关键实现说明

### 7.1 Watermark 水印

- 声明在 `event_time`（业务下单时间）上，延迟阈值 **60 秒**。
- 作用：① 允许 60 秒内的乱序/迟到数据继续参与计算；② 超过水印的迟到数据被丢弃；
  ③ 水位线之前的状态可安全清理，避免去重/聚合状态无限增长。
- 生产者有 10% 概率把 `order_time` 往前推 20~180 秒，可观察到"一部分被接纳、一部分被丢弃"。

### 7.2 Checkpoint 容错

- 路径：`/user/hadoop/checkpoint/eshop_streaming/{ods,metrics}`，放 HDFS 而非本地磁盘。
- 保存内容：Kafka offset + 聚合状态 + 去重状态。任务重启后从上次 offset 继续，做到不丢不重。
- 注意：checkpoint 目录不能随意删除，删了就要从 `startingOffsets` 重新开始，会重复消费。

### 7.3 ForeachBatch 流批复用

- 流模式：`writeStream.foreachBatch(process_batch)`，每 10 秒一个微批。
- 批模式：`run_batch()` 直接读 HDFS 历史 ODS，按天切批，调用**同一个** `process_batch()`。
- 计算逻辑只写一遍，流和批只是输入源不同。

### 7.4 去重的两个细节

1. `dropDuplicates(["dedup_key"])` 的 key 是构造出来的：真实订单号用 `order_id`，
   **订单号缺失的行用 Kafka 时间戳造唯一键**，否则所有 NULL 会被当成同一条，异常单统计不全。
2. 跨批次去重靠水印状态自动完成，批内 `dropDuplicates` 保证批量重跑幂等。

## 八、常见问题与解决

| 问题 | 原因 | 解决 |
| --- | --- | --- |
| `TypeError: Object of type Decimal is not JSON serializable` | pymysql 查 `DECIMAL` 返回 `decimal.Decimal`，Flask 默认编码器不支持 | `to_jsonable()` 递归转 float/datetime |
| `Failed to find data source: kafka` | Spark 3.3 预编译包不含 Kafka 连接器 | 把 4 个 jar 放进 `$SPARK_HOME/jars/` |
| `Table 'gx_ec_mysql.dws_eshop_hour_stats' doesn't exist` | 原实训资源包没有 MySQL 建表脚本，接口却查 3 张表 | 补 `sql/mysql_ddl.sql`，接口改为"实时表优先、DWS 兜底" |
| `NoSuchMethodError: Preconditions.checkArgument` | Hive 自带 guava-19 与 Hadoop 3.3 的 guava-27 冲突 | 用 Hadoop 的 guava-27 替换 Hive 的 guava-19 |
| `Cannot find a valid baseurl for repo: base/7` | CentOS 7 已 EOL，官方 yum 源下线 | 挂载 ISO 做本地源 / 切阿里云 vault |
| 客单价越算越小 | MySQL `ON DUPLICATE KEY UPDATE` 按顺序赋值，先更新 order_cnt 再算 avg_price | 把 `avg_price` 的赋值写在 `order_cnt` 之前 |
| Kafka 报 `node already exists ... does not match current session` | 非正常关机后 Zookeeper 残留 `/brokers/ids/0` | 杀掉残留进程重启；必要时 `delete /brokers/ids/0` |

## 九、拓展任务

**品类销量短时预测 + 品类画像聚类**（详见 `docs/拓展任务说明.txt`）

- 特征：时间序号、星期几、滞后 1 天、滞后 2 天
- 模型：多元线性回归，留出最后 2 天算 MAE
- 输出：未来 3 天各品类预测销量，写入 MySQL `ads_category_forecast`，大屏底部虚线展示
- 附加：KMeans 按（日均销量、客单价、销量波动率）把品类聚成"畅销稳定 / 高客单价 / 波动大"三类

## 十、运行结果（实测）

| 验证项 | 实际结果 |
| --- | --- |
| 集群进程 | NameNode / DataNode / SecondaryNameNode / ResourceManager / NodeManager / QuorumPeerMain(ZK) / Kafka / RunJar(Hive Metastore) 共 8 个 Java 进程 |
| Kafka Topic | `guangxi-eshop-stream`，1 分区 1 副本，Leader=0，ISR=0 |
| Hive 四层 | 6 张表：ods_eshop_order、dwd_eshop_order_valid、dwd_eshop_order_abnormal、dws_eshop_hour_stats、ads_eshop_daily_report、ads_category_forecast |
| 离线分区 | ODS/DWD/DWS/ADS 共 11 个 dt 分区（2026-10-16 ~ 2026-10-26），单日 DWD 有效单 260~416 行 |
| 实时指标 | 累计 160 单 / 成交 28068.89 元 / 异常 11 单 / 客单价 175.43 元 |
| 分钟趋势 | 5 个分钟点（20:13 ~ 20:35） |
| 预测结果 | 12 行（4 品类 × 未来 3 天），写入 ads_category_forecast |
| 接口 | `/api/health` 返回 status=ok 且列出 8 张表；`/api/eshop_data` 正常返回 JSON |
| 大屏 | 4 张指标卡 + 4 张图 + 异常告警列表全部有数据，3 秒刷新 |

> 说明：上表数字来自 4 核 6G 虚拟机（单机伪分布式）的实际运行；实时数字会随生产者持续运行增长。

## 十一、说明

- 数据全部由 `kafka_producer.py` 仿真生成，不含任何真实用户信息。
- 单机伪分布式配置，副本数均为 1，仅用于学习与小规模验证；生产环境需按集群规模调整分区、副本与内存参数。
