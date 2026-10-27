# -*- coding: utf-8 -*-
"""
品类销量预测 + 品类聚类（A组拓展任务）

做什么：
    1. 从 MySQL 的 ads_eshop_daily_report 取每日各品类销量
    2. 特征工程：时间序号、星期几、滞后1天/滞后2天销量
    3. 线性回归预测未来 3 天各品类销量，留出最后 2 天做测试算 MAE
    4. KMeans 按（客单价、日均销量、销量波动率）把品类聚成 3 类
       —— 对应任务书"销量预测 + 用户/商品画像聚类"，用于品类画像分析
    5. 预测结果写回 MySQL 表 ads_category_forecast，大屏可直接展示

运行：
    python ml_forecast.py
    python ml_forecast.py --days 5        # 预测未来 5 天
"""

import argparse
import sys
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import pymysql
from sklearn.cluster import KMeans
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_absolute_error

DB_CONFIG = {
    "host": "192.168.100.100",
    "port": 3306,
    "user": "root",
    "password": "123456",
    "database": "gx_ec_mysql",
    "charset": "utf8mb4",
}

MIN_DAYS = 4          # 一个品类至少 4 天数据才建模
HOLDOUT = 2           # 留最后 2 天做测试


def load_daily() -> pd.DataFrame:
    """取每日各品类销量：dt × category × 订单量"""
    sql = """
        SELECT dt, category, SUM(day_order_cnt) AS cnt, SUM(day_total_amount) AS amount
        FROM ads_eshop_daily_report
        GROUP BY dt, category
        ORDER BY dt, category
    """
    conn = pymysql.connect(**DB_CONFIG)
    try:
        df = pd.read_sql(sql, conn)
    finally:
        conn.close()
    if df.empty:
        raise SystemExit(
            "ads_eshop_daily_report 没有数据。\n"
            "请先跑：spark-submit offline_etl.py --dt all")
    df["dt"] = pd.to_datetime(df["dt"])
    return df


def build_features(series: pd.DataFrame) -> pd.DataFrame:
    """
    series: 单个品类按日期升序的 DataFrame[dt, cnt]
    特征：时间序号 t、星期几 weekday、lag1、lag2
    """
    s = series.sort_values("dt").reset_index(drop=True).copy()
    s["t"] = np.arange(len(s))
    s["weekday"] = s["dt"].dt.weekday
    s["lag1"] = s["cnt"].shift(1)
    s["lag2"] = s["cnt"].shift(2)
    return s.dropna().reset_index(drop=True)


def forecast_category(series: pd.DataFrame, days: int):
    """单品类：返回 (未来 days 天预测值列表, MAE, 样本数)"""
    feat = build_features(series)
    if len(feat) < MIN_DAYS:
        return None

    X = feat[["t", "weekday", "lag1", "lag2"]].values
    y = feat["cnt"].values

    # 留出法评估
    if len(feat) - HOLDOUT >= 3:
        model = LinearRegression().fit(X[:-HOLDOUT], y[:-HOLDOUT])
        mae = mean_absolute_error(y[-HOLDOUT:], model.predict(X[-HOLDOUT:]))
    else:
        model = LinearRegression().fit(X, y)
        mae = 0.0

    # 全量重训 + 递推预测
    model = LinearRegression().fit(X, y)
    last_dt = series["dt"].max()
    hist = list(series.sort_values("dt")["cnt"].values)
    preds = []
    for step in range(1, days + 1):
        t = len(hist)
        weekday = (last_dt + timedelta(days=step)).weekday()
        x = np.array([[t, weekday, hist[-1], hist[-2]]])
        p = max(int(round(float(model.predict(x)[0]))), 0)
        preds.append(p)
        hist.append(p)
    return preds, float(mae), len(feat)


def save_forecast(rows, base_dt: str):
    """预测结果写回 MySQL。

    整表重写而不是按 dt 删：预测是"当前快照"，留旧基准日的行会让大屏出现
    两套预测日期（比如 10-27 和 10-28 各一份），图表就重了。
    """
    conn = pymysql.connect(**DB_CONFIG)
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM ads_category_forecast")
            if rows:
                cur.executemany(
                    """INSERT INTO ads_category_forecast
                       (dt, category, forecast_date, forecast_cnt, model_name, mae)
                       VALUES (%s,%s,%s,%s,%s,%s)""", rows)
        conn.commit()
    finally:
        conn.close()


def cluster_categories(df: pd.DataFrame):
    """按客单价、日均销量、销量波动率做 KMeans，识别'畅销稳定/高客单/波动大'三类"""
    g = df.groupby("category").agg(
        avg_cnt=("cnt", "mean"),
        avg_price=("amount", lambda x: 0.0),   # 占位，下面用金额/销量重算
        total_amt=("amount", "sum"),
        total_cnt=("cnt", "sum"),
        std_cnt=("cnt", "std"),
    ).dropna()

    g["avg_price"] = g["total_amt"] / g["total_cnt"].replace(0, np.nan)
    g["cv"] = (g["std_cnt"] / g["avg_cnt"].replace(0, np.nan)).fillna(0)
    feats = g[["avg_cnt", "avg_price", "cv"]].fillna(0)

    if len(feats) < 3:
        print("品类太少，跳过聚类")
        return None

    k = 3 if len(feats) >= 3 else len(feats)
    km = KMeans(n_clusters=k, n_init=10, random_state=42).fit(feats)
    g["cluster"] = km.labels_

    names = {}
    for c in range(k):
        sub = g[g["cluster"] == c]
        if len(sub) == 0:
            continue
        if sub["avg_price"].mean() > g["avg_price"].mean() * 1.2:
            names[c] = "高客单价"
        elif sub["cv"].mean() > g["cv"].mean() * 1.15:
            names[c] = "销量波动大"
        else:
            names[c] = "畅销稳定"
    g["cluster_name"] = g["cluster"].map(names)

    print("\n--- 品类画像聚类（KMeans, k={}）---".format(k))
    print(g[["avg_cnt", "avg_price", "cv", "cluster_name"]]
          .round(2).sort_values("cluster_name").to_string())
    return g


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=3)
    args = parser.parse_args()

    df = load_daily()
    base_dt = df["dt"].max().strftime("%Y-%m-%d")
    print("数据范围：{} ~ {}，共 {} 个品类".format(
        df["dt"].min().strftime("%Y-%m-%d"), base_dt, df["category"].nunique()))
    print("预测未来 {} 天".format(args.days))

    rows = []
    print("\n--- 各品类预测结果 ---")
    for cat, sub in df.groupby("category"):
        res = forecast_category(sub, args.days)
        if res is None:
            print("  {:10s} 数据不足（<{}天），跳过".format(cat, MIN_DAYS + 2))
            continue
        preds, mae, n = res
        last_dt = sub["dt"].max()
        print("  {:10s} 样本{:3d}  MAE={:6.2f}  预测：{}".format(
            cat, n, mae,
            ", ".join("{}={}".format((last_dt + timedelta(days=i + 1)).strftime("%m-%d"), p)
                      for i, p in enumerate(preds))))
        for i, p in enumerate(preds):
            rows.append((base_dt, cat,
                         (last_dt + timedelta(days=i + 1)).strftime("%Y-%m-%d"),
                         p, "LinearRegression", round(mae, 2)))

    save_forecast(rows, base_dt)
    print("\n已写入 MySQL：ads_category_forecast，共 {} 行（基准日 {}）".format(len(rows), base_dt))

    cluster_categories(df)


if __name__ == "__main__":
    main()
