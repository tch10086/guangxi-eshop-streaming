# -*- coding: utf-8 -*-
"""
Structured Streaming 流批一体主任务（A组：广西特色农产品电商订单分析）

本文件是实训 35 分核心考点的落地：
    ① Watermark       —— 事件时间 60 秒水印，处理迟到/乱序订单
    ② Checkpoint      —— HDFS 持久化 offset+状态，任务重启不丢不重
    ③ ForeachBatch    —— 微批里复用同一套计算函数，实现"流批一体"

流批一体怎么体现：
    - stream 模式：消费 Kafka 无界流 → 每 10 秒一个微批 → 写 MySQL 实时表
    - batch  模式：--mode batch 读 HDFS 上的历史 ODS 数据 → 按天切批
                   → 调用 **同一个** process_batch() → 写同一套 MySQL 表
    同一份代码、同一个函数，既能流式消费，也能批量重跑历史，这就是流批一体。

提交命令（注意必须带 Kafka 连接器 4 个 jar）：
    spark-submit --master local[2] \
      --jars $SPARK_HOME/jars/spark-sql-kafka-0-10_2.12-3.3.0.jar,\
$SPARK_HOME/jars/spark-token-provider-kafka-0-10_2.12-3.3.0.jar,\
$SPARK_HOME/jars/kafka-clients-3.2.1.jar,$SPARK_HOME/jars/commons-pool2-2.11.0.jar \
      spark_streaming.py --mode stream

只跑一趟就把积压数据算完（截图/录屏方便）：
      spark_streaming.py --mode stream --once
批量重跑历史（验证流批一体）：
      spark_streaming.py --mode batch --input /user/hive/warehouse/gx_ec_hive.db/ods_eshop_order
"""

import argparse
import sys
from datetime import datetime

import pymysql
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, concat, concat_ws, count, date_format, from_json, lit,
    max as _max, round as _round, to_timestamp, trim, when, sum as _sum,
)
from pyspark.sql.types import (
    DoubleType, StringType, StructField, StructType,
)

# ============================================================
# 配置区：换环境只改这里
# ============================================================
CONFIG = {
    "kafka_bootstrap": "192.168.100.100:9092",
    "topic": "guangxi-eshop-stream",
    "watermark": "60 seconds",          # 水印延迟，容忍 60 秒内迟到
    "trigger_interval": "10 seconds",   # 微批间隔
    "checkpoint": "/user/hadoop/checkpoint/eshop_streaming",
    "ods_path": "/user/hive/warehouse/gx_ec_hive.db/ods_eshop_order",
    "mysql": {
        "host": "192.168.100.100",
        "port": 3306,
        "user": "root",
        "password": "123456",
        "database": "gx_ec_mysql",
        "charset": "utf8mb4",
    },
}

# 订单 JSON Schema（与 kafka_producer.py 严格一致）
ORDER_SCHEMA = StructType([
    StructField("order_id", StringType(), True),
    StructField("buyer_id", StringType(), True),
    StructField("product_code", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("city", StringType(), True),
    StructField("order_time", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("address", StringType(), True),
    StructField("tag", StringType(), True),
])

# HDFS 上 ODS 的 TSV 结构：与订单 Schema 一致，
# dt 列由 HDFS 分区目录 dt=yyyy-MM-dd 自动发现，不要在 schema 里重复声明
ODS_TSV_SCHEMA = ORDER_SCHEMA


# ============================================================
# 一、SparkSession
# ============================================================
def build_spark(app_name: str) -> SparkSession:
    return (SparkSession.builder
            .appName(app_name)
            .master("local[2]")
            .config("spark.sql.shuffle.partitions", "4")
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            # 关掉 UI 上没用的堆栈，日志干净一点，截图好看
            .config("spark.ui.showConsoleProgress", "false")
            .getOrCreate())


# ============================================================
# 二、MySQL 工具
# ============================================================
def get_conn():
    return pymysql.connect(**CONFIG["mysql"])


def truncate_realtime_tables():
    """批模式重跑前清空实时表，保证结果可复现"""
    sqls = [
        "TRUNCATE TABLE realtime_order_index",
        "TRUNCATE TABLE realtime_order_minute",
        "TRUNCATE TABLE realtime_category_index",
        "TRUNCATE TABLE realtime_city_index",
        "TRUNCATE TABLE abnormal_order_log",
        "INSERT INTO realtime_order_index "
        "(id, order_cnt, total_amount, abnormal_cnt, avg_price, order_time) "
        "VALUES (1, 0, 0.00, 0, 0.00, NOW())",
    ]
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            for s in sqls:
                cur.execute(s)
        conn.commit()
    finally:
        conn.close()


def write_mysql(batch_id, valid_cnt, valid_amount, abnormal_cnt,
                stat_minute, agg_rows, abnormal_rows):
    """
    一个微批的结果落 MySQL：
      - 实时总表累加（UPSERT）
      - 分钟趋势累加
      - 品类/地市排行累加
      - 异常订单明细插入
    """
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            # 1) 实时总表：注意 avg_price 必须写在累加之前，
            #    否则 MySQL 会先用更新后的 order_cnt 算客单价（顺序陷阱）
            cur.execute(
                """
                INSERT INTO realtime_order_index
                    (id, order_cnt, total_amount, abnormal_cnt, avg_price, order_time)
                VALUES (1, %s, %s, %s,
                        ROUND(%s / NULLIF(%s, 0), 2), %s)
                ON DUPLICATE KEY UPDATE
                    avg_price    = ROUND((total_amount + VALUES(total_amount))
                                         / NULLIF(order_cnt + VALUES(order_cnt), 0), 2),
                    order_cnt    = order_cnt + VALUES(order_cnt),
                    total_amount = total_amount + VALUES(total_amount),
                    abnormal_cnt = abnormal_cnt + VALUES(abnormal_cnt),
                    order_time   = VALUES(order_time)
                """,
                (valid_cnt, valid_amount, abnormal_cnt,
                 valid_amount, valid_cnt, stat_minute))

            # 2) 分钟趋势
            cur.execute(
                """
                INSERT INTO realtime_order_minute (stat_minute, order_cnt, total_amount)
                VALUES (%s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    order_cnt    = order_cnt + VALUES(order_cnt),
                    total_amount = total_amount + VALUES(total_amount)
                """,
                (stat_minute, valid_cnt, valid_amount))

            # 3) 品类排行 / 地市分布
            if agg_rows:
                cur.executemany(
                    """
                    INSERT INTO realtime_category_index (category, order_cnt, total_amount)
                    VALUES (%s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        order_cnt    = order_cnt + VALUES(order_cnt),
                        total_amount = total_amount + VALUES(total_amount)
                    """,
                    [(r["category"], r["cnt"], r["amt"]) for r in agg_rows])

                cur.executemany(
                    """
                    INSERT INTO realtime_city_index (city, order_cnt, total_amount)
                    VALUES (%s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        order_cnt    = order_cnt + VALUES(order_cnt),
                        total_amount = total_amount + VALUES(total_amount)
                    """,
                    [(r["city"], r["cnt"], r["amt"]) for r in agg_rows])

            # 4) 异常明细
            if abnormal_rows:
                cur.executemany(
                    """
                    INSERT INTO abnormal_order_log
                        (order_id, buyer_id, product_name, category, city,
                         amount, address, abnormal_reason, order_time)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    [(r["order_id"], r["buyer_id"], r["product_name"], r["category"],
                      r["city"], r["amount"], r["address"], r["abnormal_reason"],
                      r["order_time"]) for r in abnormal_rows])
        conn.commit()
    finally:
        conn.close()


# ============================================================
# 三、★ 流批共用的计算核心 ★
#    流模式由 foreachBatch 调用；批模式由 main_batch 直接调用
# ============================================================
def process_batch(batch_df, batch_id: int, replay: bool = False):
    if batch_df.rdd.isEmpty():
        # 没有新数据就静默跳过：每 10 秒刷一行"空批次"会把演示画面弄乱
        return

    # ---------- 1) 去重 ----------
    # 跨批次去重由流上的 dropDuplicates + 水印状态完成；
    # 批内再去一次，保证 batch 重跑也幂等。
    # 注意：order_id 为 NULL 的行要单独处理，
    # 否则所有 NULL 会被当成"同一条"而只留一条，异常单就统计不全了。
    not_null = batch_df.filter(col("order_id").isNotNull()).dropDuplicates(["order_id"])
    null_rows = batch_df.filter(col("order_id").isNull())
    df = not_null.unionByName(null_rows)

    # ---------- 2) 打异常原因标签 ----------
    labeled = df.withColumn(
        "abnormal_reason",
        when(col("order_id").isNull(), lit("订单ID缺失"))
        .when(col("amount").isNull() | (col("amount") <= 0), lit("负金额/金额非法"))
        .when(col("address").isNull() | (trim(col("address")) == ""), lit("收货地址为空"))
        .when(col("event_time").isNull(), lit("时间格式非法"))
        .otherwise(lit(None)))

    valid = labeled.filter(col("abnormal_reason").isNull())
    abnormal = labeled.filter(col("abnormal_reason").isNotNull())

    # ---------- 3) 聚合 ----------
    agg = (valid.groupBy("category", "city")
           .agg(count("*").alias("cnt"),
                _round(_sum("amount"), 2).alias("amt"))
           .collect())

    valid_cnt = sum(r["cnt"] for r in agg)
    valid_amount = round(sum(float(r["amt"]) for r in agg), 2)
    abnormal_cnt = abnormal.count()

    # 本批业务时间 → 分钟（没有有效数据就退回当前时间）
    max_ts = valid.agg(_max("event_time")).collect()[0][0]
    stat_minute = (max_ts or datetime.now()).replace(second=0, microsecond=0)

    abnormal_rows = [r.asDict() for r in
                     abnormal.select("order_id", "buyer_id", "product_name", "category",
                                     "city", "amount", "address", "abnormal_reason",
                                     "order_time")
                     .limit(200).collect()]

    # ---------- 4) 落库 ----------
    write_mysql(batch_id, valid_cnt, valid_amount, abnormal_cnt,
                stat_minute, [r.asDict() for r in agg], abnormal_rows)

    mode = "BATCH" if replay else "STREAM"
    print("[{} batch {}] 有效单 {} 笔 ¥{} | 异常 {} 笔 | 业务时间 {}".format(
        mode, batch_id, valid_cnt, valid_amount, abnormal_cnt, stat_minute))


# ============================================================
# 四、流模式
# ============================================================
def read_kafka_stream(spark):
    raw = (spark.readStream
           .format("kafka")
           .option("kafka.bootstrap.servers", CONFIG["kafka_bootstrap"])
           .option("subscribe", CONFIG["topic"])
           .option("startingOffsets", "latest")
           .option("maxOffsetsPerTrigger", 500)   # 控制单批数据量
           .option("failOnDataLoss", "false")
           .load())

    parsed = (raw.select(
        from_json(col("value").cast("string"), ORDER_SCHEMA).alias("d"),
        col("value").cast("string").alias("raw_json"),
        col("timestamp").alias("kafka_ts"))
        .select("d.*", "raw_json", "kafka_ts")
        .withColumn("event_time", to_timestamp(col("order_time"), "yyyy-MM-dd HH:mm:ss"))
        .withColumn("dt", date_format(col("event_time"), "yyyy-MM-dd")))

    # ★ 水印 + 去重 ★
    # dedup_key：真实订单号用 order_id；订单号缺失的用 Kafka 时间戳造唯一键，
    # 这样既能对真实重复单去重，又不会把"缺失ID"的异常单误判成同一条。
    deduped = (parsed
               .withColumn("dedup_key",
                           when(col("order_id").isNull(),
                                concat(lit("NULL-"), col("kafka_ts").cast("string")))
                           .otherwise(col("order_id")))
               .withWatermark("event_time", CONFIG["watermark"])
               .dropDuplicates(["dedup_key"]))
    return deduped


def run_stream(spark, once: bool = False):
    deduped = read_kafka_stream(spark)

    # ---- 查询1：原始数据落 HDFS，作为离线数仓 ODS ----
    ods_cols = [col("order_id"), col("buyer_id"), col("product_code"),
                col("product_name"), col("category"), col("city"),
                col("order_time"), col("amount").cast("string"),
                col("address"), col("tag")]
    ods_line = concat_ws("\t",
                         *[when(c.isNull(), lit("\\N")).otherwise(c) for c in ods_cols])
    ods_writer = (deduped.select(ods_line.alias("value"), col("dt"))
                  .writeStream
                  .format("text")
                  .option("path", CONFIG["ods_path"])
                  .option("checkpointLocation", CONFIG["checkpoint"] + "/ods")
                  .partitionBy("dt")
                  .outputMode("append"))

    # ---- 查询2：实时指标 → foreachBatch → MySQL ----
    metrics_writer = (deduped.writeStream
                      .foreachBatch(lambda df, bid: process_batch(df, bid))
                      .option("checkpointLocation", CONFIG["checkpoint"] + "/metrics")
                      .outputMode("append"))

    if once:
        # availableNow：把当前积压的数据全部处理完就自动停，适合截图/录屏
        q_ods = ods_writer.trigger(availableNow=True).start()
        q_metrics = metrics_writer.trigger(availableNow=True).start()
        q_ods.awaitTermination()
        q_metrics.awaitTermination()
        print("=== --once 模式：积压数据处理完毕 ===")
    else:
        ods_writer.start()
        metrics_writer.trigger(processingTime=CONFIG["trigger_interval"]).start()
        print("流任务已启动：水印 {} | 微批 {} | checkpoint {}".format(
            CONFIG["watermark"], CONFIG["trigger_interval"], CONFIG["checkpoint"]))
        spark.streams.awaitAnyTermination()


# ============================================================
# 五、批模式（流批一体的"批"）
# ============================================================
def run_batch(spark, input_path: str, reset: bool = True):
    """
    批模式：把 HDFS 上历史 ODS 数据按天切批，调用**同一个** process_batch()。
    批模式没有水印状态，重复单号靠批内 dropDuplicates 去重。
    """
    if reset:
        truncate_realtime_tables()
        print("已清空实时表，开始重跑历史")

    df = (spark.read
          .option("sep", "\t")
          .option("nullValue", "\\N")
          .schema(ODS_TSV_SCHEMA)
          .csv(input_path))                      # dt 由分区目录自动发现
    df = (df
          .withColumn("event_time", to_timestamp(col("order_time"), "yyyy-MM-dd HH:mm:ss"))
          .filter(col("event_time").isNotNull()))

    days = sorted([r["dt"] for r in df.select("dt").distinct().collect()])
    if not days:
        print("输入路径没有数据：{}".format(input_path))
        return

    print("批模式：共 {} 个分区 {}".format(len(days), days))
    for i, d in enumerate(days, start=1):
        process_batch(df.filter(col("dt") == d), i, replay=True)
    print("=== 批模式重跑完成，共 {} 批 ===".format(len(days)))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="A组电商 Structured Streaming 流批一体任务")
    parser.add_argument("--mode", choices=["stream", "batch"], default="stream")
    parser.add_argument("--once", action="store_true", help="流模式只跑一趟积压数据")
    parser.add_argument("--input", default=CONFIG["ods_path"], help="批模式输入路径")
    parser.add_argument("--no-reset", action="store_true", help="批模式不清空实时表")
    args = parser.parse_args()

    spark = build_spark("GX_Eshop_Streaming")
    # 日志只留 ERROR：WARN 级别的 HDFSBackedStateStoreProvider 会在每个微批刷一行，
    # 把 process_batch 打印的批次结果淹掉
    spark.sparkContext.setLogLevel("ERROR")

    if args.mode == "stream":
        run_stream(spark, once=args.once)
    else:
        run_batch(spark, args.input, reset=not args.no_reset)
