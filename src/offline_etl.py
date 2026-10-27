# -*- coding: utf-8 -*-
"""
SparkSQL 离线数仓 ETL（A组：广西特色农产品电商订单分析）

分层流转：
    ODS  ods_eshop_order            ← 流任务把 Kafka 原始 JSON 落成 TSV，按 dt 分区
     ↓  清洗（金额>0、地址非空、订单ID非空、批内去重）+ 打异常标签
    DWD  dwd_eshop_order_valid / dwd_eshop_order_abnormal
     ↓  按 品类 × 地市 × 小时 聚合
    DWS  dws_eshop_hour_stats
     ↓  按 品类 × 地市 × 天 汇总 + 客单价
    ADS  ads_eshop_daily_report
     ↓  JDBC / PyMySQL 同步
    MySQL  dws_eshop_hour_stats、ads_eshop_daily_report（供大屏离线板块）

运行：
    spark-submit --master local[2] offline_etl.py --dt 2026-10-26
    spark-submit --master local[2] offline_etl.py --dt all      # 全量重跑
"""

import argparse
import sys

import pymysql
from pyspark.sql import SparkSession

CONFIG = {
    "hive_db": "gx_ec_hive",
    "hive_warehouse": "/user/hive/warehouse",
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


def build_spark() -> SparkSession:
    return (SparkSession.builder
            .appName("GX_Eshop_OfflineETL")
            .master("local[2]")
            .config("spark.sql.warehouse.dir", CONFIG["hive_warehouse"])
            .config("spark.sql.shuffle.partitions", "4")
            .config("hive.exec.dynamic.partition", "true")
            .config("hive.exec.dynamic.partition.mode", "nonstrict")
            .enableHiveSupport()
            .getOrCreate())


def mysql_conn():
    return pymysql.connect(**CONFIG["mysql"])


def sync_to_mysql(spark, dt: str):
    """把本分区 DWS/ADS 结果同步到 MySQL（先删后插，保证幂等）"""
    dws = spark.sql("SELECT * FROM {}.dws_eshop_hour_stats WHERE dt='{}'"
                    .format(CONFIG["hive_db"], dt)).collect()
    ads = spark.sql("SELECT * FROM {}.ads_eshop_daily_report WHERE dt='{}'"
                    .format(CONFIG["hive_db"], dt)).collect()

    conn = mysql_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM dws_eshop_hour_stats WHERE dt=%s", (dt,))
            cur.execute("DELETE FROM ads_eshop_daily_report WHERE dt=%s", (dt,))
            if dws:
                cur.executemany(
                    """INSERT INTO dws_eshop_hour_stats
                       (dt, hour, category, city, order_cnt, total_amount, abnormal_cnt)
                       VALUES (%s,%s,%s,%s,%s,%s,%s)""",
                    [(r["dt"], r["hour"], r["category"], r["city"],
                      int(r["order_cnt"] or 0), float(r["total_amount"] or 0),
                      int(r["abnormal_cnt"] or 0)) for r in dws])
            if ads:
                cur.executemany(
                    """INSERT INTO ads_eshop_daily_report
                       (dt, category, city, day_order_cnt, day_total_amount, abnormal_cnt, avg_price)
                       VALUES (%s,%s,%s,%s,%s,%s,%s)""",
                    [(r["dt"], r["category"], r["city"],
                      int(r["day_order_cnt"] or 0), float(r["day_total_amount"] or 0),
                      int(r["abnormal_cnt"] or 0), float(r["avg_price"] or 0)) for r in ads])
        conn.commit()
    finally:
        conn.close()
    print("[{}] 同步 MySQL 完成：DWS {} 行 / ADS {} 行".format(dt, len(dws), len(ads)))


def run(spark, target_dt: str):
    spark.sql("USE {}".format(CONFIG["hive_db"]))
    # 让 Hive 感知流任务新写入的分区
    spark.sql("MSCK REPAIR TABLE ods_eshop_order")

    dts = ([r[0] for r in spark.sql("SHOW PARTITIONS ods_eshop_order").collect()]
           if target_dt == "all" else ["dt=" + target_dt])
    print("待处理分区：", dts)

    for p in dts:
        dt = p.split("=")[1]
        print("\n================ 处理分区 {} ================".format(dt))

        # ---------------- DWD：清洗 + 去重 + 异常分流 ----------------
        spark.sql("""
            INSERT OVERWRITE TABLE dwd_eshop_order_valid PARTITION (dt='{dt}')
            SELECT order_id, buyer_id, product_code, product_name, category, city,
                   order_time, amount, address,
                   substr(order_time, 12, 2) AS order_hour
            FROM (
                SELECT *, row_number() OVER (PARTITION BY order_id ORDER BY order_time) AS rn
                FROM ods_eshop_order
                WHERE dt='{dt}'
                  AND order_id IS NOT NULL
                  AND amount > 0
                  AND address IS NOT NULL AND trim(address) <> ''
                  AND order_time IS NOT NULL
            ) t
            WHERE rn = 1
        """.format(dt=dt))

        spark.sql("""
            INSERT OVERWRITE TABLE dwd_eshop_order_abnormal PARTITION (dt='{dt}')
            SELECT order_id, buyer_id, product_name, category, city,
                   order_time, amount, address,
                   CASE
                     WHEN order_id IS NULL THEN '订单ID缺失'
                     WHEN amount IS NULL OR amount <= 0 THEN '负金额/金额非法'
                     WHEN address IS NULL OR trim(address) = '' THEN '收货地址为空'
                     ELSE '其他'
                   END AS abnormal_reason
            FROM ods_eshop_order
            WHERE dt='{dt}'
              AND (order_id IS NULL
                   OR amount IS NULL OR amount <= 0
                   OR address IS NULL OR trim(address) = '')
        """.format(dt=dt))

        # ---------------- DWS：小时 × 品类 × 地市 ----------------
        spark.sql("""
            INSERT OVERWRITE TABLE dws_eshop_hour_stats PARTITION (dt='{dt}')
            SELECT v.order_hour AS hour, v.category, v.city,
                   COUNT(*) AS order_cnt,
                   ROUND(SUM(v.amount), 2) AS total_amount,
                   COALESCE(a.abnormal_cnt, 0) AS abnormal_cnt
            FROM dwd_eshop_order_valid v
            LEFT JOIN (
                SELECT category, city, COUNT(*) AS abnormal_cnt
                FROM dwd_eshop_order_abnormal WHERE dt='{dt}'
                GROUP BY category, city
            ) a ON v.category = a.category AND v.city = a.city
            WHERE v.dt='{dt}'
            GROUP BY v.order_hour, v.category, v.city, a.abnormal_cnt
        """.format(dt=dt))

        # ---------------- ADS：天 × 品类 × 地市 + 客单价 ----------------
        spark.sql("""
            INSERT OVERWRITE TABLE ads_eshop_daily_report PARTITION (dt='{dt}')
            SELECT category, city,
                   SUM(order_cnt) AS day_order_cnt,
                   ROUND(SUM(total_amount), 2) AS day_total_amount,
                   SUM(abnormal_cnt) AS abnormal_cnt,
                   ROUND(SUM(total_amount) / NULLIF(SUM(order_cnt), 0), 2) AS avg_price
            FROM dws_eshop_hour_stats
            WHERE dt='{dt}'
            GROUP BY category, city
        """.format(dt=dt))

        # ---------------- 校验 + 同步 MySQL ----------------
        for t in ("dwd_eshop_order_valid", "dwd_eshop_order_abnormal",
                  "dws_eshop_hour_stats", "ads_eshop_daily_report"):
            n = spark.sql("SELECT COUNT(*) c FROM {} WHERE dt='{}'".format(t, dt)).collect()[0]["c"]
            print("  {:28s} {:>8d} 行".format(t, n))

        sync_to_mysql(spark, dt)


def main():
    parser = argparse.ArgumentParser(description="A组电商离线数仓 ETL")
    parser.add_argument("--dt", default="all", help="分区日期 yyyy-MM-dd，或 all 全量")
    args = parser.parse_args()

    spark = build_spark()
    # 只留 ERROR，避免 Hive/Spark 的 WARN 刷屏把 ETL 结果行淹掉
    spark.sparkContext.setLogLevel("ERROR")
    try:
        run(spark, args.dt)
        print("\n=== 离线 ETL 全部完成 ===")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
